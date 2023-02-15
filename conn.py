#!/usr/bin/env python

import os

lcl_connection_str = raw_input("Please enter connection string: ")
lcl_username = raw_input("username: ")
lcl_passwd = raw_input("password: ")
print "you entered", lcl_connection_str
print "you entered", lcl_username
print "you entered", lcl_passwd
lcl_query="select count(1) from dual"

sqoopcom="sqoop eval  --connect " + lcl_connection_str +" --username "  + lcl_username + " --password  " +lcl_passwd + " --query \" "+ lcl_query +" \" "
print sqoopcom
return_val=os.system(sqoopcom +" &>/dev/null")
print return_val
