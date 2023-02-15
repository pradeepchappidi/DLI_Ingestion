#!/usr/bin/env python

"""
This script will find resume all Oozie coordinators for a given user or list of IDs.
"""


from __future__ import print_function
import sys
import os
from argparse import ArgumentParser
import oozie_lib as oozie


def main(argv=None):
    """
    Run the program.
    """
    # Parse Arguments
    source, user, list_file = parse_args(argv)

    # Get a list of coordinator IDs from the specified source
    if source == 'user':
        coord_ids = query_oozie(user)
        print('Found {} suspended coordinators for user {}.'.format(len(coord_ids), user))
    elif source == 'list':
        coord_ids = read_list_file(list_file)
        print('Found {} coordinator IDs in: {}'.format(len(coord_ids), list_file))
    else:
        print('ERROR: "Impossible" Case. Program should not reach here.')
        sys.exit(1)

    # Resume all coordinators
    print('Resuming Coordintors...')
    for cid in coord_ids:
        resume_coord(cid)

    print('Done.')

    return 0


def parse_args(argv):
    """
    Parse command line options.
    """
    if argv is None:  # No arguments supplied. Likely called from terminal.
        argv = sys.argv

    parser = ArgumentParser(prog=argv[0], description='Resume all Oozie coordinators in a given list or for a given user.')
    # Use a mutex group. We want exactly one of the arguments.
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '-u', '--user', nargs=1,
        help='All suspended coordinators will be resumed for the given user.'
    )
    group.add_argument(
        '-l', '--list', nargs=1,
        help='All coordintators in the given list will be resumed.'
    )

    args = vars(parser.parse_args(args=argv[1:]))

    if args['user'] is not None:
        source = 'user'
        user = args['user'][0]
        list_file = None
    elif args['list'] is not None:
        source = 'list'
        list_file = os.path.abspath(args['list'][0])
        user = None
    else:
        print('ERROR: "Impossible" Case. Program should not reach here.')
        sys.exit(1)

    return (source, user, list_file)


def query_oozie(user):
    """
    Use the oozie API to look up the id of every suspended coordinator for a given user.
    """
    coord_ids = []  # Keep track of all returned coord ids
    opts = ['jobtype=coord', 'filter=status=SUSPENDED;user={}'.format(user)]
    for page in oozie.get_yield_json('jobs', options=opts):
        # For every API response, extract and save the coordinator IDs.
        coord_ids.extend([j['coordJobId'] for j in page['coordinatorjobs']])

    return coord_ids


def read_list_file(list_file):
    """
    Read coordinator IDs from a file, where each coordinator ID is on it's own line.
    """
    with open(list_file, 'r') as f:
        coord_ids = [l.strip() for l in f.readlines()]
    return coord_ids


def resume_coord(coord_id):
    """
    Make an API call to resume the given coordinator ID.
    """
    # Make API call
    oozie.put('job/{}'.format(coord_id), options=['action=resume'])


# Actually run the script if calling from the command line
if __name__ == '__main__':
    sys.exit(main())
