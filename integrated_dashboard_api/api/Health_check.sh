Health check 
#!/bin/bash
################################################################################################
# ##############################################################################################
# initialize_constants #
# ##############################################################################################
function initialize_constants {

    pDate="`date +%Y%m%d`"
    pDateTime=`date '+%d%m%Y_%H:%M:%S'`
    pLog="$log_dir/system_monitor"
    cLog="$log_dir/cpu_monitor"
    mLog="$log_dir/memory_monitor"
    host=`hostname`

}




# ##############################################################################################
# set_parameters #
# ##############################################################################################
function set_params {
    retCode=0

    if [ -z $log_dir ]
    then
        echo "Log dir is mandatory parameter, please check the config"
        retCode=-1;
    else
        if [ -d $log_dir ]
        then
            echo "Log dir ($log_dir) exists. "
        else
            mkdir $log_dir
            if [ $? -ne 0 ]
            then
                echo " Error while creating the log dir - $log_dir"
                retCode=-1;
            fi

            chmod 750 $log_dir
            if [ $? -ne 0 ]
            then
                echo " Error while setting permissions to the log dir - $log_dir"
            fi

        fi
    fi



    if [ -z $email_id ]
    then
        echo "Mail Id is mandatory parameter, please check the config"
        retCode=-1;
    else
        echo " Email Id set to : ${email_id}"
    fi



    if [ -z $Threshold ]
    then
        echo "Threshold not provided. Defaulting to 80%"
    else
        echo " Threshold set to : ${Threshold}"
    fi



    if [ -z $nfs_mounts ]
    then
        echo "nfs mounts not provided. NFS health will not be checked."
    else
        echo " nfs_mounts set to : ${nfs_mounts}"
    fi


    return ${retCode}
}


# ##############################################################################################
# get_edgeNode_usage_stats #
# ##############################################################################################
function get_edgeNode_usage_stats {
    #Memory Usage
    memoryusage=$(free -m | awk 'NR==2{printf "Memory Usage: %s/%sMB (%.2f%%)\n", $3,$2,($3)*100/$2 }' | cut -d '(' -f 2 | cut -d '%' -f 1 | cut -d '.' -f 1)



    #Memory Stats
    if [ "$memoryusage" -ge "$Threshold" ]; then
      mtasks=$(ps -eo pid,ppid,user,cmd,%mem,etime, --sort=-%mem | head -20)
      memorytasks=$(echo -e "Memory Consuming Taks : \n""======================\n""$mtasks")
      echo -e "Memory Consuming Taks - ${pDateTime} \n `ps -eo pid,ppid,user,%mem,etime,cmd --sort=-%mem | head -20`" >> ${mLog}_${pDateTime}.log
    fi

    #Disk Usage
    diskusage=$(df /u | grep / | awk '{ print $5}' | sed 's/%//g' | tail -1)

    #Disk Stats
    if [ "$diskusage" -ge "$Threshold" ]; then
      dstat=0
      diskstats=0
      dstat=$(du --separate-dirs -h /u/ | sort -rh | head -20)
      diskstats=$(echo -e "Disk Stats : \n""============\n""$dstat")
    fi


    #CPU Usage
    CPUusage=$(grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage "%"}' | cut -d '.' -f 1)

    #CPU Stats
    if [ "$CPUusage" -ge "$Threshold" ]; then
      ctasks=$(ps -eo pid,ppid,user,cmd,%cpu,etime --sort=-%cpu | head -20)
      cputasks=$(echo -e "Tasks consuming CPU : \n""=====================\n""$ctasks")
      echo -e "Tasks consuming CPU - ${pDateTime} \n `ps -eo pid,ppid,user,%cpu,etime,cmd --sort=-%cpu | head -20`" >> ${cLog}_${pDateTime}.log
    fi


    echo "${pDateTime} :: Memory Usage: ${memoryusage}%; Disk Usage: ${diskusage}%; CPU Usage: ${CPUusage}%;" >> ${pLog}_${pDate}.log

}


# ##############################################################################################
# get_NFS_usage_stats #
# ##############################################################################################
function get_NFS_usage_stats {


    for i in $(echo ${nfs_mounts} | sed "s/,/ /g")
    do
        echo "Checking usage stats for NFS Mount : ${i}"

        #Disk Usage
        diskusage_nfs_1=$(df ${i} | grep / | awk '{ print $5}' | sed 's/%//g' | tail -1)

        #Disk Stats
        if [ "$diskusage_nfs_1" -ge "$Threshold" ]; then
          dstat_nfs_1=0
          diskstats_nfs_1=0
          dstat_nfs_1=$(du --separate-dirs -h ${i}/ | sort -rh | head -20)
          diskstats_nfs_1=$(echo -e "\n \n Disk Stats : $i: \n""============\n""$dstat_nfs_1")
        fi

        if [ ${diskusage_nfs_1} -ge ${Threshold} ] ; then
            NFS_Usage_GT_Threshold=1
        fi

        diskusage_nfs="${diskusage_nfs}     $i:$diskusage_nfs_1 %"
        diskstats_nfs="${diskstats_nfs} $diskstats_nfs_1"

        echo "${pDateTime} :: Disk Usage: ${i} - ${diskusage_nfs_1}%; " >> ${pLog}_${pDate}.log

    done


}

# ##############################################################################################
# send_mail_usage_stats #
# ##############################################################################################
function send_mail_usage_stats {
    retCode=0

    #dummy=$(mpstat)
    if [ "$memoryusage" -ge "$Threshold" -o "$diskusage" -ge "$Threshold" -o "$CPUusage" -ge "$Threshold" -o "$NFS_Usage_GT_Threshold" -eq 1 ]; then

        if [ "$memoryusage" -ge "$Threshold" ]; then
         mem_filename=$(echo "-a ""${mLog}_${pDateTime}.log")
        else
         mem_filename=' '
        fi


        if [ "$CPUusage" -ge "$Threshold" ]; then
         cpu_filename=$(echo "-a ""${cLog}_${pDateTime}.log")
        else
         cpu_filename=' '
        fi


        mailx  $mem_filename $cpu_filename -s 'Alert from '$host''  ${email_id} << EOF

Hi All,

Please find the utilization report for $host below
/u/ mountpint disk usage is: $diskusage%
Memory Usage is: $memoryusage%
CPU usage is: $CPUusage%
${diskusage_nfs}

#We have set the threshold for all the above parameters to  $Threshold%.
One of the above parameters has reached threshold of $Threshold%. Please investigate with the below details:
$diskstats

$memorytasks

$cputasks

$diskstats_nfs
EOF


        if [ $? -ne 0 ]
        then
                echo " Script Failed while sending the mail! "
                retCode=-1;
        else
                echo " Mail sent !"
        fi
    fi

    return ${retCode}
}






# ##############################################################################################
# MAIN PROGRAM #
# ##############################################################################################
if [ $# -ne 1 ]
then
        echo " Wrong number of parameters passed!"
        echo " usage $0 <config file with path>"
        exit -1;
fi

CONFIG_FILE=${1}
Return_Code=0
NFS_Usage_GT_Threshold=0
diskusage_nfs=""
diskstats_nfs=""


if [ -f ${CONFIG_FILE} ]
then
        echo "Config File : ${CONFIG_FILE} exists. "
else
        echo " Config file - $1 does not exists! "
        exit -1;
fi


#Source the config file
source ${CONFIG_FILE}

initialize_constants
Return_Code=$?
if [ $Return_Code -ne 0 ]; then
    echo "Error while initializing constants. Exiting..."
    exit $Return_Code
fi

echo "Script execution started ${pDateTime}"


set_params
Return_Code=$?
if [ $Return_Code -ne 0 ]; then
    echo "Error while reading and setting parameters. Exiting..."
    exit $Return_Code
fi


get_edgeNode_usage_stats

if [ ! -z ${nfs_mounts} ] ; then
diskusage_nfs="NFS Mount Usage: "
get_NFS_usage_stats
fi

send_mail_usage_stats
Return_Code=$?
if [ $Return_Code -ne 0 ]; then
    echo "Error while sending usage statistics. Exiting..."
    exit $Return_Code
fi



echo " Script execution Completed @`date`"


(END)
