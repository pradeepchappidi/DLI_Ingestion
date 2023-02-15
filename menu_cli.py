#!/usr/bin/env python


import os
from optparse import OptionParser

__version__ = '0.0.1'

def option_setup():
    usage = u'%prog'
    parser = OptionParser(usage = usage, version = __version__)
    parser.add_option('-T', '--TestConn',
            action = 'store_true',
            dest = 'is_testconn',
            help = 'test connection'
    )

    parser.add_option('-f', '--file',
            action = 'store_true',
            dest = 'is_file',
            help = 'takes file path'
    )

    parser.add_option('-t', '--tables',
            action = 'store_true',
            dest = 'is_tables',
            help = 'provide comma seperated tables list'
    )

    parser.add_option('-R', '--rdbms_type',
            action = 'store',
            type = 'string',
            dest = 'is_rdbms_type',
            help = 'provide the RDBMS ingestion source type , like ORACLE, DB2, MYSQL etc'
    )

    parser.add_option('-C', '--ftp_crontab',
            action = 'store_true',
            dest = 'is_ftp_crontab',
            help = 'takes how often it has to schedule for polling'
    )

    parser.add_option('-H', '--ftp_hive_struc',
            action = 'store_true',
            dest = 'is_ftp_create_hive_structure',
            help = 'takes file location where the strcture is defined for hive to create table of that'
    )


    parser.set_defaults(
            is_testconn = False,
            is_tables = False,
            is_file = False,
            is_rdbms_type = False,
            is_ftp_crontab = False,
            is_ftp_create_hive_structure = False 
    )



    options, args = parser.parse_args()
    
    return options

    #helloto = ''
    #if options.is_testconn:
    #    helloto = 'testconn'
    #
    #if options.is_file:
    #    helloto = 'file'
    #
    #if options.is_tables:
    #    helloto = 'tables'
    #
    #print "hello %s" % helloto


def testConnection(test_query):
   lcl_connection_str = raw_input("Please enter connection string: ")
   lcl_username = raw_input("username: ")
   lcl_passwd = raw_input("password: ")
   
   # lcl_query="select count(1) from dual"

   sqoopcom="sqoop eval  --connect " + lcl_connection_str +" --username "  + lcl_username + " --password  " +lcl_passwd + " --query \" "+ test_query +" \" "
   print sqoopcom
   return_val=os.system(sqoopcom +" &>/dev/null")
   print return_val


def rdbms_type():
   options=option_setup() 
   if options.is_rdbms_type:
      if options.is_rdbms_type == "ORACLE" :
         oracle_test_sql = " select count(1) from dual "
         testConnection(oracle_test_sql);
      if options.is_rdbms_type == "MYSQL" :
         mysql_test_sql = " select count(1) from dual "
         testConnection(mysql_test_sql);
      if options.is_rdbms_type == "DB2" :
         db2_test_sql = " select count(1) from dual "
         testConnection(db2_test_sql);
      else :
         print " Wrong RDBMS type provided "
         exit()

 
def ftp_setup():
   options=option_setup()
   if options.is_ftp_crontab:
       ftp_ingest_frequency = raw_input("Please enter how often you like to move files to HDFS from FTP location alltimes in mins ( ex like 30 mins, 10 mins, 60 mins etc): ")
       ftp_input_name = raw_input(" specify FTP source name ( like Media, if this is media room data ; DRP , if this is for drp system etc) : " )
       ftp_src_file_landing_loc = raw_input(" Provide the Location where the FTP files will land : " );
       ftp_trg_file_push_loc = raw_input(" Provide where in HDFS we like these files to be moved : ")
       ftp_command = "./ftp_pattern_ksh " + ftp_input_name + "  " + ftp_src_file_landing_loc + "  " + ftp_trg_file_push_loc + " " +"SCREEN_AND_LOG"
       print ftp_command
       return_val=os.system(ftp_command)
       raw_input(" ");
   if options.is_ftp_create_hive_structure:
       is_ftp_structure_create = raw_input(" Do you like to create Hive Table Structure[Y/N] : " )
       if is_ftp_structure_create == "Y" :
         ftp_structure_file = raw_input( " please provide the location where you have the struture defined : " );
       else:
         exit() 
   else:
       print "Wrong option for FTP Ingestion"
       exit()

if __name__ == '__main__':
    rdbms_type()
    ftp_setup()
