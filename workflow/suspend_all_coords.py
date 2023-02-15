#!/usr/bin/env python

"""
This script will find a suspend all Oozie coordinators for a given user.
"""


from __future__ import print_function
import sys
import os
from argparse import ArgumentParser
from datetime import datetime
import oozie_lib as oozie


def main(argv=None):
    """
    Run the program.
    """
    # Parse Arguments
    user, output_dir = parse_args(argv)

    # Create the output file and directory
    output_file = create_output_file(output_dir)

    # Query the Oozie API
    coord_ids = query_oozie(user)
    print('Found {} running coordinators for user {}.'.format(len(coord_ids), user))

    # Suspend all coordinators
    print('Suspending Coordintors. Check the output file to see which coordinators are suspended.')
    print('Output File: {}'.format(output_file))
    with open(output_file, 'w') as f:
        for cid in coord_ids:
            suspend_coord(cid, f)

    print('Done.')

    return 0


def parse_args(argv):
    """
    Parse command line options.
    """
    if argv is None:  # No arguments supplied. Likely called from terminal.
        argv = sys.argv

    parser = ArgumentParser(prog=argv[0], description='Suspend all Oozie coordinators for the given user.')
    parser.add_argument(
        'user',
        help='The user to suspend coordinators for.'
    )
    parser.add_argument(
        '-o', '--output-dir',
        action='store', default='./output', required=False,
        help='Where to output the list of suspended coordinators.'
    )

    args = vars(parser.parse_args(args=argv[1:]))

    user = args['user']
    output_dir = os.path.abspath(args['output_dir'])

    return (user, output_dir)


def create_output_file(output_dir):
    """
    Create the output file and directory.
    """
    # Create the directory if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Make sure the directory is really a directory
    if not os.path.isdir(output_dir):
        print('ERROR: {} is not a directory.'.format(output_dir))
        sys.exit(2)

    # We want to generate a timestamp in the file name so multiple runs don't overwrite each other.
    timestamp = datetime.utcnow().strftime('%Y.%m.%d.%H.%M.%S.%f')
    file_name = 'suspended_coords_{}.txt'.format(timestamp)
    output_file = os.path.join(output_dir, file_name)

    return output_file


def query_oozie(user):
    """
    Use the oozie API to look up the id of every running coordinator for a given user.
    """
    coord_ids = []  # Keep track of all returned coord ids
    opts = ['jobtype=coord', 'filter=status=RUNNING;user={}'.format(user)]
    for page in oozie.get_yield_json('jobs', options=opts):
        # For every API response, extract and save the coordinator IDs.
        coord_ids.extend([j['coordJobId'] for j in page['coordinatorjobs']])

    return coord_ids


def suspend_coord(coord_id, output_file):
    """
    Make an API call to suspend the given coordinator ID,
    and output the ID to the output file.
    """
    # Make API call
    oozie.put('job/{}'.format(coord_id), options=['action=suspend'])
    # Write ID to file
    output_file.write('{}\n'.format(coord_id))


# Actually run the script if calling from the command line
if __name__ == '__main__':
    sys.exit(main())
