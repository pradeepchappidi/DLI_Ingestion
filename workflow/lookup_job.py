#!/usr/bin/env python

"""
This script will find a list of Oozie jobs with a given name, and return information
about them, including their ID.
"""


from __future__ import print_function
import sys
import re
from argparse import ArgumentParser, ArgumentTypeError
from collections import namedtuple
import oozie_lib as oozie


class Table:
    """
    Represents a table of results, and methods for printing that table to the screen.
    Each row is a dictionary.
    """
    Column = namedtuple('Column', 'name, key, width')

    def __init__(self, rows):
        self.rows = rows
        self.columns = []

    def add_column(self, column_name, column_key):
        """
        Add a column to the table. The column_key is the key of the column in the underlying dict.
        """
        self.columns.append(self.Column(
            column_name,
            column_key,
            self.__column_width(map(lambda row: row[column_key], self.rows))  # Calc the max width of the column being added
        ))

    @staticmethod
    def __column_width(items):
        """
        Given a list of items, determine the maximum width of a printed column.
        """
        return max(map(len, items))

    def print_table(self):
        """
        Print the table to standard output
        """
        # A format string of the form "| {:<#} | {:<#} | ..." where each # is the max column width
        format_str = '|' + '|'.join(map(lambda c: ' {{:<{}}} '.format(c.width), self.columns)) + '|'
        # A horizontal bar of the form "+-----+------+...." where each column is the correct width
        horiz_bar = '+' + '+'.join(map(lambda c: '-' * (c.width + 2), self.columns)) + '+'

        # Recall that f(*args) passes in the list args as positional arguments to f.
        # So args = [1, 'foo'] => f(*args) = f(1, 'foo')
        # Print the table.
        print(horiz_bar)
        print(format_str.format(*(map(lambda c: c.name, self.columns))))  # Print the column names.
        print(horiz_bar)
        for row in self.rows:
            print(format_str.format(*(map(lambda c: row[c.key], self.columns))))  # Print the row values
        print(horiz_bar)


def main(argv=None):
    """
    Run the program.
    """
    # Parse Arguments
    opts = parse_args(argv)

    # Turn the job name into a regex search pattern.
    if opts['use_regex']:
        search_pattern = opts['job_name']
    else:
        search_pattern = '.*' + re.escape(opts['job_name']) + '.*'

    # Depending on the job type, query the API print the result of the search.
    if opts['job_type'] == 'wf':
        results = list(extract_wf_matches(search_pattern, opts))
        print_results_wf(results)
    elif opts['job_type'] == 'coord':
        results = list(extract_coord_matches(search_pattern, opts))
        print_results_coord(results)
    elif opts['job_type'] == 'bundle':
        results = list(extract_bundle_matches(search_pattern, opts))
        print_results_bundle(results)
    else:
        print('ERROR: Unexpected job_type - Cannot parse', file=sys.err)
        return 1

    return 0


def parse_args(argv):
    """
    Parse command line options.
    """
    if argv is None:  # No arguments supplied. Likely called from terminal.
        argv = sys.argv

    parser = ArgumentParser(prog=argv[0], description='Look up Oozie job properties given its name.')
    parser.add_argument(
        'job_name',
        help='The name of the Oozie job.'
    )
    parser.add_argument(
        '-n',
        action='store', default=None, required=False, type=gt_zero_int,
        help='Limit the amount of results displayed to the specified value.'
    )
    parser.add_argument(
        '-s', '--status',
        action='store', default=None, required=False,
        help='Only search jobs in the given status.'
    )
    parser.add_argument(
        '-t', '--type',
        action='store', default='wf', choices=('wf', 'coord', 'bundle'), required=False,
        help='Only search jobs of the given type. Default: wf'
    )
    parser.add_argument(
        '-z', '--timezone',
        action='store', default='America/Chicago', required=False,
        help='Specify the time zone of results, such as GMT. Default: America/Chicago'
    )
    parser.add_argument(
        '-re',
        action='store_true', required=False,
        help='Flag to interpret the job name as a Python regular expression.'
    )

    args = vars(parser.parse_args(args=argv[1:]))

    opts = {
        'job_name': args['job_name'],
        'job_status': args['status'],
        'job_type': args['type'],
        'num_results': args['n'],
        'timezone': args['timezone'],
        'use_regex': args['re'],
    }

    return opts


def gt_zero_int(value):
    """
    Check if the value is greater than zero.
    Helper function for the argument parser.
    """
    ivalue = int(value)
    if ivalue <= 0:
        raise ArgumentTypeError('{} is not greater than zero.'.format(ivalue))
    return ivalue


def query_oozie(job_type, job_status, timezone):
    """
    Use the oozie API to look up job information matching the specified criteria.
    The is a generator function, and will yield pages from the API response.
    """
    # Set options for the API request
    opts = []
    opts.append('jobtype={}'.format(job_type))
    if job_status is not None:
        opts.append('filter=status={}'.format(job_status))
    opts.append('timezone={}'.format(timezone))

    # Make the API request and yield each page as it's asked for.
    for page in oozie.get_yield_json('jobs', options=opts):
        yield page


def extract_matches(search_pattern, jobs_extractor, name_extractor, opts):
    """
    Query Oozie, loop through each result page and extract jobs that match the search pattern.
    The jobs extractor function should return the job list when given an API response page.
    The name extractor should extract the job name given a job.
    This function is a generator, and will return macthes until there is no more data or
    the maximum number of matches is reached.
    """
    num_matches = 0
    limit = opts['num_results']

    # Query Oozie, and loop through each response page
    for page in query_oozie(opts['job_type'], opts['job_status'], opts['timezone']):
        # Extract the list of jobs from the response
        jobs = jobs_extractor(page)
        for job in jobs:
            # Check if the job name matches the search pattern
            if re.match(search_pattern, name_extractor(job)):
                # Yield the match
                yield job

                # Increment the number of matches and check if we hit the limit.
                num_matches += 1
                if limit is not None and num_matches >= limit:
                    raise StopIteration  # We hit the limit, stop the generator.


def extract_wf_matches(search_pattern, opts):
    """
    A shorthand to return an extract_matches generator parameterized for workflows.
    """
    return extract_matches(
        search_pattern,
        lambda page: page['workflows'],  # Extracts workflow list from response page
        lambda job: job['appName'],  # Extract workflow name
        opts
    )


def extract_coord_matches(search_pattern, opts):
    """
    A shorthand to return an extract_matches generator parameterized for coordinators.
    """
    return extract_matches(
        search_pattern,
        lambda page: page['coordinatorjobs'],  # Extracts coord list from response page
        lambda job: job['coordJobName'],  # Extract coord name
        opts
    )


def extract_bundle_matches(search_pattern, opts):
    """
    A shorthand to return an extract_matches generator parameterized for bundles.
    """
    return extract_matches(
        search_pattern,
        lambda page: page['bundlejobs'],  # Extracts bundle list from response page
        lambda job: job['bundleJobName'],  # Extract bundle name
        opts
    )


def print_results_wf(results):
    """
    Print results in a tabular format.
    """
    if len(results) == 0:
        print('NO RESULTS')
        return

    # Print results
    table = Table(results)
    table.add_column('NAME', 'appName')
    table.add_column('ID', 'id')
    table.add_column('STATUS', 'status')
    table.add_column('USER', 'user')
    table.add_column('CREATED', 'createdTime')
    table.print_table()


def print_results_coord(results):
    """
    Print results in a tabular format.
    """
    if len(results) == 0:
        print('NO RESULTS')
        return

    # Print results
    table = Table(results)
    table.add_column('NAME', 'coordJobName')
    table.add_column('ID', 'coordJobId')
    table.add_column('STATUS', 'status')
    table.add_column('USER', 'user')
    table.add_column('FREQ', 'frequency')
    table.add_column('START TIME', 'startTime')
    table.print_table()


def print_results_bundle(results):
    """
    Print results in a tabular format.
    """
    # Apply the given search filter
    if len(results) == 0:
        print('NO RESULTS')
        return

    # Print results
    table = Table(results)
    table.add_column('NAME', 'bundleJobName')
    table.add_column('ID', 'bundleJobId')
    table.add_column('STATUS', 'status')
    table.add_column('USER', 'user')
    #table.add_column('START TIME', 'startTime')
    table.print_table()


# Actually run the script if calling from the command line
if __name__ == '__main__':
    sys.exit(main())
