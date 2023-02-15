#!/usr/bin/env python

"""
This script will softly kill an Oozie coordinator.
This involves:
    (1) AS input, get a coordinator ID or name.
        (a) If a name, look up the ID. We want to find a running, non-duplicate instance of the name.
        (b) If an ID, see if it is running.
    (2) If the coordinator is paused or suspended, kill it and return.
    (3) Assuming we have a running coordinator, apply a "soft kill" by setting the end time to now.
    (4) Wait for the coordinator to enter a completed state.
    (5) Check the status of coordinator's last workflow action.

"""


from __future__ import print_function
import sys
from datetime import datetime
from time import sleep
from argparse import ArgumentParser, ArgumentTypeError
import oozie_lib as oozie


# A list of states that indicate an Oozie Coordinator is "finished".
# There are inconsitencies in the documentation over the spelling of "DONEWITHERROR", so include both versions.
COORD_FINISHED_STATES = ['SUCCEEDED', 'DONWITHERROR', 'DONEWITHERROR', 'KILLED', 'FAILED']
# A list of states that indicate an Oozie Coordinator is paused or suspended. (Not running.)
COORD_SUSPENDED_STATES = ['PREPSUSPENDED', 'SUSPENDED', 'SUSPENDEDWITHERROR', 'PREPPAUSED', 'PAUSED', 'PAUSEDWITHERROR']


def main(argv=None):
    """
    Run the program.
    """
    # Parse Arguments
    opts = parse_args(argv)

    # We take different actions depending on if we were passed a coordinator ID or name.
    if opts['mode'] == 'NAME':
        # Passed a name. Check if there is 1 active coord with that name.
        try:
            coord_id = get_active_coord_id(opts['name'])
            if coord_id is None:  # No matches!
                print('Found no active coordinators with name "{}".'.format(opts['name']))
                return 0
        except DuplicateNameException as e:  # Multiple matches!
            print(e.message, file=sys.stderr)
            sys.exit(1)
    elif opts['mode'] == 'ID':
        # Passed an ID. Check to see if it is finished.
        coord_id = opts['id']
        if check_if_finished(coord_id):
            print('{} is in a finished state, no need to kill.'.format(coord_id))
            return 0
    else:
        # Something went wrong, we should never reach this case.
        print('Impossible Program Mode! ({})'.format(opts['mode']), file=sys.stderr)
        sys.exit(2)

    # If the code reaches this point, we have a running or suspended coordinator ID
    if check_if_suspended(coord_id):
        print('{} is in a suspended/paused state. Applying a hard kill.'.format(coord_id))
        hard_kill_coord(coord_id)
        return 0

    # The coordinator is running, so we need to apply a soft kill
    print('Applying soft kill to {}.'.format(coord_id))
    soft_kill_coord(coord_id)

    # If the no wait option was specified we are done
    if opts['no_wait']:
        print('No-Wait flag specified. Returning now instead of waiting for coordinator to finish.')
        return 0

    # Otherwise, wait for the coorinator to finish
    print('Waiting for {} to enter a finished state...'.format(coord_id))
    coord_status = wait_for_finish(coord_id, opts['delay'])
    print('{} finished with an overall status of {}'.format(coord_id, coord_status))

    # Check the status of the last workflow action to make sure the job ended okay.
    success, wf_status = check_last_workflow_status(coord_id)
    if success:
        print('The last action of {} was successful.'.format(coord_id))
        return 0
    else:
        print('The last action of {} was not successful: {}'.format(coord_id, wf_status))
        return 1


def parse_args(argv):
    """
    Parse command line options.
    """
    if argv is None:  # No arguments supplied. Likely called from terminal.
        argv = sys.argv

    # Configure the argument parser
    parser = ArgumentParser(prog=argv[0], description='Softly kill a running coordinator by setting the end time to "now".')
    # Use a mutex group. We want exactly one of the arguments.
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--id', action='store', help='The ID for an Oozie coordinator.'
    )
    group.add_argument(
        '--name', action='store', help='The name of an Oozie coordinator.'
    )
    parser.add_argument(
        '--no-wait', action='store_true', required=False,
        help='Apply a soft kill, but do not wait for coordinator completion or check it\'s last workflow status.'
    )
    parser.add_argument(
        '--delay', action='store', default=15, required=False, type=gt_zero_int,
        help='How often to check if a coordinator is finished, in seconds.'
    )

    # Parse the arguments
    args = vars(parser.parse_args(args=argv[1:]))

    # Figure out whether name or id was supplied
    if args['name'] is not None:
        args['mode'] = 'NAME'
    elif args['id'] is not None:
        args['mode'] = 'ID'
    else:
        print('Impossible Case in Argument Parser!', file=sys.stderr)
        sys.exit(2)

    return args


def gt_zero_int(value):
    """
    Check if the value is greater than zero.
    Helper function for the argument parser.
    """
    ivalue = int(value)
    if ivalue <= 0:
        raise ArgumentTypeError('{} is not greater than zero.'.format(ivalue))
    return ivalue


class DuplicateNameException(Exception):
    """
    Exception raised when multiple active coordinators with the same name are found.
    """
    def __init__(self, coord_name, coord_ids):
        self.message = 'Multiple ({}) active cordinators with name "{}" found:\n{}'.format(
            len(coord_ids),
            coord_name,
            '\n'.join(coord_ids)
        )


def get_active_coord_id(coord_name):
    """
    Given the name of an active (not finished) coordinator, find it's ID.
    Return None if there are no matches.
    Raise an error if duplicates are found.
    """
    # Query the Oozie coordinator list, and for each returned page keep track
    # of any active coordinator IDs from coordinators with a matching name
    matches = []
    for page in oozie.get_yield_json('jobs', options=['jobtype=coord']):
        for coord in page['coordinatorjobs']:
            if coord['coordJobName'] == coord_name and coord['status'] not in COORD_FINISHED_STATES:
                matches.append(coord['coordJobId'])

    # Return an appropriate response or error out based on the number of matches.
    if len(matches) == 0:
        return None
    elif len(matches) != 1:
        raise DuplicateNameException(coord_name, matches)
    else:
        return matches[0]


def check_if_finished(coord_id):
    """
    Check if the given coordinator is in a finished state.
    """
    # Make the API request
    response = oozie.get_json('job/{}'.format(coord_id), ['show=info'])
    # Check to see if the coordinator is in a finished state
    return response['status'] in COORD_FINISHED_STATES


def check_if_suspended(coord_id):
    """
    Check if the given coordinator is in a suspended or paused state.
    """
    # Make the API request
    response = oozie.get_json('job/{}'.format(coord_id), ['show=info'])
    # Check to see if the coordinator is in a finished state
    return response['status'] in COORD_SUSPENDED_STATES


def hard_kill_coord(coord_id):
    """
    Explicitly kill a coordinator.
    """
    oozie.put('job/{}'.format(coord_id), options=['action=kill'])


def soft_kill_coord(coord_id):
    """
    Apply a "soft kill" to the target coordinator ID by setting it's end time to "now".
    """
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%MZ')  # Format current UTC time string
    # Set API endpoint and options
    endpoint = 'job/{}'.format(coord_id)
    options = ['action=change', 'value=endtime={}'.format(now)]
    # Make the API call to set the end time and effectively apply a soft kill
    oozie.put(endpoint, options=options)


def wait_for_finish(coord_id, poll_delay):
    """
    Wait for a coordinator to enter a finished state.
    """
    # Set API request paramaters
    endpoint = 'job/{}'.format(coord_id)
    options = ['show=info']

    while True:
        # Make an API call to retrieve the current status
        status = oozie.get_json(endpoint, options=options)['status']
        # Finised yet? If so, we can exit the loop. Otherwise we can wait and try again.
        if status in COORD_FINISHED_STATES:
            return status
        else:
            sleep(poll_delay)


def check_last_workflow_status(coord_id):
    """
    Check the status of a coordinator's last workflow and make sure it was successful.
    Return a tuple of (success, status).
    """
    # Set API request paramaters
    endpoint = 'job/{}'.format(coord_id)
    options = ['show=info', 'order=desc', 'len=1']

    # Make an API call to retrieve the last coordinator action
    actions = oozie.get_json(endpoint, options=options)['actions']

    # Retrieve the action status
    if actions:  # There was a last workflow
        status = actions[0]['status']
    else:  # Empty list, so the coord never ran any worklows. Return successfully.
        return (True, 'NO_ACTIONS')

    # Check if the status is successful
    success = (status == 'SUCCEEDED')
    return (success, status)


# Actually run the script if calling from the command line
if __name__ == '__main__':
    sys.exit(main())
