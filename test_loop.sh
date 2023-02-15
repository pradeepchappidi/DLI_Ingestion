#for file_name in `ls -lrt \`pwd\`/FTP/*.DAT* | awk '{print $9 }' `
#do
#   echo `basename ${file_name}`
#done 


check_if_a_process_exists()
{

  PIDFILE='/home/PXCHAPP/DLI'
  echo ${PIDFILE} 
}


   rtn_value=$(check_if_a_process_exists)
   echo "rtn_value is ${rtn_value} "
 
