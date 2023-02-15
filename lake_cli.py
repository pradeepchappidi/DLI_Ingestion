#!/usr/bin/env python

"""
This script will calculate runtime statistics for an Ooozie coordinator.
"""


#from __future__ import print_function
#from __future__ import division
import sys
import argparse

#from argparse import ArgumentParser
#from collections import namedtuple
#from datetime import datetime, timedelta
#import oozie_lib as oozie

import oozie_errors

version='1.0'



def parse_command_line(argv):
    """Parse command line argument. See -h option
    :param argv: arguments on the command line must include caller file name.
    """
    formatter_class = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description="command line interface for rrd_process for customer letters extraction into solar for search",
                                     formatter_class=formatter_class)
  
    parser.add_argument("--version", action="version",
                        version="%(prog)s {}".format(version))
    parser.add_argument("-v", "--verbose", dest="verbose_count",
                        action="count", default=0,
                        help="increases log verbosity for each occurence.")
    parser.add_argument("-o", "--output",metavar="output",
                        type=argparse.FileType('w'), default=sys.stdout,
                        help="redirect output to a file")
    parser.add_argument("-u","--url", metavar="URL",
                        required=True,
                        help="URL for the oozie where we need to get the api results to run from")
    parser.add_argument("-w","--wf_id", metavar="workflow id",
                        #required=True,
                        help="Get data for a specific workflow id")
    parser.add_argument("-c","--cor_id", metavar="co-ordinator id",
                        #required=True,
                        help="Get data for a specific co-ordinator id")
    parser.add_argument("-a","--all", metavar="",
                        #required=True,
                        help="Get data for all workflows and co-ordinators")
    parser.add_argument("-d","--delimiter", metavar="DELIMITER",
                        #required=True,
                        help="Data delimiter for output data. default is set to comma")
    parser.add_argument("-t","--type", metavar="TYPE",
                        #required=True,
                        help="output data type valid values can be csv, json, xml. default is csv ")
    parser.add_argument("-T","--TestConn", metavar="",
                        #required=True,
                        help="Please provide values for username password DB_Type connection_string values for this parameter to be functional")
 
    parser.add_argument("-f","--file", metavar="file_name",
                        #required=True,
                        help="provide the file_name with absolute location, if not will look up in local dir ")

    parser.add_argument("-l","--ListofTables", metavar="TABLES_NAMES",
                        #required=True,
                        help="provide list of tablenames with comma seperated, these are the only tables that will be ingested")

    parser.add_argument("-S","--Sqoop_Options", metavar="",
                        #required=True,
                        help="provide sqoop options in pairs with comma as delimiter ex import=yes, connection=connectionstr, hive-database=prad etc, if they are null then properties file has to be given ")

    parser.add_argument("-P","--properties_file", metavar="",
                        #required=True,
                        help="either give the sqoop options from a command prompt with -S option or provide them in a file. the file has to be given with -f option")


    
    #check to see if there any arguments given , if not run a help on the command to will give some 
	#verbiage for people on how to use.
    if(len(argv[1:]) == 0) : 
      arguments = parser.parse_args(['-h']) 
    else:
      arguments = parser.parse_args(argv[1:])
	 
    # Sets log level to WARN going more verbose for each new -v.
    	
    return (arguments , parser)
  
    







def main(argv):
    """
    Run the program.
    """
    # Parse Arguments and the returned name space is turned into a dict by using var
    (cmd_args, input_parser) = parse_command_line(argv)
    
    cmd_args_dict=vars(cmd_args)
    oozie_url = cmd_args_dict['url']
    wf_id= cmd_args_dict['wf_id']
    cor_id= cmd_args_dict['cor_id']
    all_jobs= cmd_args_dict['all']
    output_data_type= cmd_args_dict['type']
    output_data_delim= cmd_args_dict['delimiter']
    try: 
		if wf_id!= None:
		   url_extension = 'job/{}'.format(wf_id)
		elif cor_id!=None:
		   url_extension = 'job/{}'.format(cor_id)
		elif all_jobs!=None:
		   url_extension = 'jobs'
		elif output_data_type==None:
		   output_data_type='csv'
		elif output_data_type!=None and output_data_type not in ('csv','json','xml'):
		   raise Exception("INPUT_PARSING_ERROR") 
		elif output_data_delim==None:
		   output_data_delim=','
		elif output_data_delim!=None: 
		   output_data_delim='{}'.format(output_data_delim)
		else:
		   raise Exception("INPUT_PARSING_ERROR")
    except:   
                print "###########################"
	        print "Error in input parameters, job_id/cor_id/all is one of the fields need to be populated for data results"
	        print "###########################"
	        input_parser.parse_args(['-h'])
		  
    print url_extension 
    options = ['show=info', 'timezone=GMT']
    
    """
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
    """
	


# Actually run the script if calling from the command line
if __name__ == '__main__':
    sys.exit(main(sys.argv))
