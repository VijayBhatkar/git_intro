#!/bin/bash
if [ $# -ne 7 ]
then
  echo " Wrong number of parameters passed, $#!"
  echo " sh long_running_dpaas_clusters.sh PROD 120 mdsefdprd us-central1 Gollapalle.Sudhakar@walmart.coms fd-persistent|mdsefdnprd-prod-dpaas-phs /edge_data/auth/$USER/auth.json "
  exit -1;
fi
echo " Script execution started @`date`"
env=$1
timeout=$2
teamspace=$3
region=$4
recipients=$5
ignore=$6
SA_AUTH_FILE="$7"
if [ -f $SA_AUTH_FILE ]; then
   if [[ `gcloud config list account --format "value(core.account)"` != `jq -r '.client_email' $SA_AUTH_FILE`  ]]; then
      gcloud auth activate-service-account --key-file=$SA_AUTH_FILE
        gcloud config set account `jq -r '.client_email' $SA_AUTH_FILE`
   fi
   export GOOGLE_APPLICATION_CREDENTIALS="$SA_AUTH_FILE"
fi
clusters=`gcloud dataproc clusters list --region ${region} --project wmt-bfdms-${teamspace} --filter='status.state = RUNNING' | awk '{if (NR!=1) {print $1 }}' | grep -vE ${ignore}`
echo -e "############### Current running clusters in $env teamspace $teamspace are: "
echo -e "$clusters"
current_time=`date -u +"%Y-%m-%dT%H:%M:%SZ"`
long_runners=("" )
for cluster_name in $clusters; do
    start_time=`gcloud dataproc clusters describe $cluster_name --region ${region} --project wmt-bfdms-${teamspace} | grep stateStartTime | head -1 | awk '{ print $2 }' | tr -d \' `
    #time_diff_mins=`date -u -d @$(($(date -d "$current_time" '+%s') - $(date -d "$start_time" '+%s'))) '+%H'`
    CURRENT=$(date +%s -d "$start_time")
    TARGET=$(date +%s -d "$current_time" )
    time_diff_mins=$(( ($TARGET - $CURRENT) / 60 ))
    if [ "$time_diff_mins" -gt "$timeout" ]; then
       #echo '****************************************************************************************'
       #echo -e "\tLong running clusters: $cluster_name for $time_diff_mins mins "
       clust="$cluster_name: $time_diff_mins mins \n"
       long_runners+=($clust)
       #echo '****************************************************************************************'
    fi
done
if [ "$(echo -ne ${long_runners[@]} | wc -m)" -eq 0 ]
then
      echo -e "\nNo Long running clusters available"
else
      echo ${long_runners[@]}
      echo -e "\nHello Team,\n \nLong running DPAAS Clusters:\n  \n${long_runners[@]} \n\n Please take appropriate action if cluster is not needed! \n\nThanks \nFlightDeck" | mutt -s "DPAAS LONG RUNNING CLUSTERS IN FlightDeck $env (${teamspace}) " "$recipients"
fi
if [ $? -ne 0 ]; then
       echo '****************************************************************************************'
	   echo                         abnormal termination of program
	   echo                         exiting...
	   echo '****************************************************************************************'
	   exit 1
else
       echo '****************************************************************************************'
       echo                            "Script ended succesfully"
       echo '****************************************************************************************'
fi
