#!/usr/bin/env python

"""
This script will calculate runtime statistics for an Ooozie coordinator.
"""


from __future__ import print_function
from __future__ import division
import sys
from argparse import ArgumentParser
from collections import namedtuple
from datetime import datetime, timedelta
import oozie_lib as oozie


def main(argv=None):
    """
    Run the program.
    """
    # Parse Arguments
    coord_id = parse_args(argv)

    # Query the Oozie API
    api_resp = query_oozie(coord_id)

    # Parse the API response
    coord_name, complete_actions, running_actions = parse_response(api_resp)

    # Calculate statistics
    mean, std_dev = calc_stats(complete_actions)

    # Print Results
    print('ID:                 {}'.format(coord_id))
    print('Name:               {}'.format(coord_name))
    print('Mean Run Time:      {}'.format(timedelta(seconds=mean)))
    print('Run Time Std. Dev.: {}'.format(timedelta(seconds=std_dev)))
    if running_actions:
        print('Currently Running Jobs:')
        for action in running_actions:
            print('    Action:      {}'.format(action.number))
            print('    Workflow ID: {}'.format(action.workflow_id))
            print('    Duration:    {}'.format(datetime.utcnow() - action.start_time))

    return 0


def parse_args(argv):
    """
    Parse command line options.
    """
    if argv is None:  # No arguments supplied. Likely called from terminal.
        argv = sys.argv

    parser = ArgumentParser(prog=argv[0], description='Calculate run time statistics for a given Oozie coordinator.')
    parser.add_argument(
        'coord_id',
        help='The Oozie coordinator\'s ID.'
    )

    args = vars(parser.parse_args(args=argv[1:]))

    coord_id = args['coord_id']

    return coord_id


def query_oozie(job_id):
    """
    Use the oozie API to look up job information
    """
    endpoint = 'job/{}'.format(job_id)
    options = ['show=info', 'timezone=GMT']
    return oozie.get_json(endpoint, options=options)


def parse_response(api_resp):
    """
    Parse the Oozie API response
    """
    CoordAction = namedtuple('CoordAction', 'number, status, workflow_id, start_time, end_time')
    coord_name = api_resp['coordJobName']

    def parse_action(action):
        """
        Inner function used to parse the JSON for a coordinator action
        """
        # Get coordinator-level info for this action
        number = action['actionNumber']
        status = action['status']
        workflow_id = action['externalId']

        # Query the actual workflow action for time info
        workflow_api_resp = query_oozie(workflow_id)
        to_datetime = lambda s: datetime.strptime(s[5:], '%d %b %Y %H:%M:%S %Z') if s is not None else None
        start_time = to_datetime(workflow_api_resp['startTime'])
        end_time = to_datetime(workflow_api_resp['endTime'])

        # Create and return result
        return CoordAction(number, status, workflow_id, start_time, end_time)

    complete_actions = [parse_action(a) for a in api_resp['actions'] if a['status'] == 'SUCCEEDED']
    running_actions = [parse_action(a) for a in api_resp['actions'] if a['status'] == 'RUNNING']
    return (coord_name, complete_actions, running_actions)


def calc_stats(actions):
    """
    Caculate statistics for completed actions.
    """
    durations = [(a.end_time - a.start_time).seconds for a in actions]
    mean = sum(durations) / len(durations)
    std_dev = (sum([(d - mean) ** 2 for d in durations]) / len(durations)) ** 0.5
    return (mean, std_dev)


# Actually run the script if calling from the command line
if __name__ == '__main__':
    sys.exit(main())
