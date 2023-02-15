#!/usr/bin/env python

"""
This script will calculate runtime statistics for an Ooozie coordinator.
"""


#from __future__ import print_function
#from __future__ import division
import sys
import argparse

import requests
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
	
	
class job(object):
   pass
   """   __init__(self):
        self.status
        self.startTime
	self.endTime
	self.name
	self.id
	self.parentId
	self.userId
	self.hdfs_path
	"""
	
	
class workflow(job):
   pass
   """ __init__(self):
        self.status
        self.startTime
	self.endTime
	self.name
	self.id
	self.parentId
	self.userId
	self.hdfs_path"""


class co_ordinator(job):
   pass
   """ __init__(self): 
        self.status
	self.startTime
	self.endTime
	self.name
	self.id
	self.parentId
	self.userId
	self.hdfs_path"""
	
class oozie_client(object):
   def __init__(self,oozie_url=None):

        self._url=None;
        self._oozie_version='v1'
        self._SSL_CERT = '/opt/cloudera/security/pki/rootca.cert.pem'
        self._kerberos_auth = HTTPKerberosAuth(mutual_authentication=REQUIRED, force_preemptive=True)

        if oozie_url is None:
            oozie_url = os.environ.get("OOZIE_URL")
        if oozie_url is None:
            raise ClientError("No Oozie Url provided and none set in the environment varialbe  OOZIE_URL")
        self._url= url.rsplit('/')
   
   #getter       
   @property
   def _kerberos_auth(self):
        return self._kerberos_auth
    
   #getter
   @property
   def _ssl_cert(self):
        return self._SSL_CERT
   
   def _serverCheck(self):
       response=requests.get(
                  url = '/'.join([self._url, self._oozie_version, 'admin/status']),
                  auth=self._kerberos_auth, 
                  verify=self._SSL_CERT
                )
       response = requests.get(request_str, auth=kerberos_auth, verify=SSL_CERT)

   def getOozieUrl(self):
       pass

   def getWfJobsInfo(self):
       pass   
   def getCorJobsInfo(self):
       pass   
   def getWfInfo(self):
       pass   
   def getCorInfo(self):
       pass   
   
   ## to do list
   """
   def killJob()
   def reRunJob()
   def suspendJob()
   def resumeJob()
   def runJob()
   def submitJob()
   def getBundleInfo()
   def getBundlesInfo()
   def wrtieToCsv()
   def writeToJson()
   def writeToXml()
   
   
   """
   #def get_json(self,url_filler=self.url_filler, options=[]):
   """
    Execute a single GET request against the Oozie API and check the response code.
    Return the response as a JSON object.

    endpoint: The API endpoint to use.
    options: A list of request options.

    Example: /oozie/v1/<job/job-id><jobs>?show=info&timezone=GMT
             ^--------^^-----------------^ ^-------^ ^----------^
             Base      job_filler  ......... Option    Option
   """
   # Format the full request string
   #self.url_to_run = '{base}{job_filler}{options}'.format(
   #                    self.oozie_url=BASE_URL,
   #                    job_filler=job_filler,
   #                    options=('?'+'&'.join(options)) if options else ''
   #                    )
   
   #return response.json()


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
    sys.exit(main(sys.argv))
