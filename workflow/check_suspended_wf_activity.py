#!/usr/bin/env python

"""
This script will find running workflows that still have running actions.
"""


from __future__ import print_function
from argparse import ArgumentParser
import sys
import oozie_lib as oozie


def main(argv=None):
    """
    Run the program.
    """
    # Parse Arguments
    parse_args(argv)

    print('Checking for suspended workflows with running actions...')

    # Query Oozie for all suspended worklows
    workflow_ids = query_oozie()

    # Filter workflows to retrieve only the ones with running actions
    active_workflow_ids = [wf for wf in workflow_ids if has_running_actions(wf)]

    # Display the results
    if active_workflow_ids:
        print('The following workflows still have running actions:')
        for wf in active_workflow_ids:
            print(wf)
    else:
        print('No suspended coordinators have running actions.')

    return 0


def parse_args(argv):
    """
    Parse command line options.
    """
    if argv is None:  # No arguments supplied. Likely called from terminal.
        argv = sys.argv

    parser = ArgumentParser(prog=argv[0], description='List suspended worklows with running actions')
    parser.parse_args(args=argv[1:])


def query_oozie():
    """
    Use the oozie API to look up all suspended workflows
    """
    workflow_ids = []
    opts = ['jobtype=wf', 'filter=status=SUSPENDED']
    for page in oozie.get_yield_json('jobs', options=opts):  # Paginated API requests
        # Extract and save the workflow IDs for each page of suspended workflows.
        workflow_ids.extend([j['id'] for j in page['workflows']])

    return workflow_ids


def has_running_actions(workflow_id):
    """
    Use the oozie API to see if a workflow has a running action
    """
    # Make an API call to get workflow information
    response = oozie.get_json('job/{}'.format(workflow_id), options=['show=info'])

    # Loop through the actions in the response, and check for a RUNNING status
    for action in response['actions']:
        if action['status'] == 'RUNNING':
            return True
    return False


# Actually run the script if calling from the command line
if __name__ == '__main__':
    sys.exit(main())
