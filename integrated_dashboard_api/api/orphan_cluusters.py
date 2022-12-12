#! /usr/bin/env python3
import cx_Oracle
import logging
import sys
import datetime
import re
import traceback
import re
import os
import yaml
import connectDB
import argparse
import paramiko
import subprocess
from time import gmtime, strftime
from datetime import datetime
from pprint import pprint
import smtplib

print("Start")
tss = []
svc_ids = []
all_cls = []
#parser = argparse.ArgumentParser()
#parser.add_argument("--ts", type=str, required=True)
#args = parser.parse_args()
# print('Hello,', args.jobs)
#teamspace = ("wmt-bfdms-"+args.ts)
#print(teamspace)
orphan_yml = "/Users/vn51hqy/GIT/Prod_Test/scripts/orphan_config.yml"
with open(orphan_yml, "r") as fp:
    orphan_conf = yaml.safe_load(fp)
#test1 = orphan_conf['dpassPara']['env'] a
#print(test1)
for k, v in orphan_conf["svcMap"].items():
    tss.append(k)
    svc_ids.append(v)
print(100 * "_")
print()
for t in tss:
    
    env=orphan_conf['dpassPara']['env']
    timeout=orphan_conf['dpassPara']['timeout']
    teamspace= t 
    region=orphan_conf['dpassPara']['region']
    recipients=orphan_conf['dpassPara']['recipients']
    ignore=orphan_conf['dpassPara']['ignore']
    if teamspace not in tss:
        print("Teamspace is not onbarded to DFS ")
    else:
        user = orphan_conf['svcMap'][teamspace]
        SA_AUTH_FILE=f"/Users/vn51hqy/GIT/Prod_Test/auth/{user}/auth.json"
    #print(SA_AUTH_FILE)
    #print(SA_AUTH_FILE)
    cmd = f"gcloud auth activate-service-account --key-file={SA_AUTH_FILE}"
    #print(cmd)


    a, b = subprocess.getstatusoutput(cmd)
    
    if ( a == 0):
        print(f"Service Account is activated for Teamspace :- {t}")
    else:
        print("Not able to activated Service account, Please verify auth key")

    first = f"gcloud dataproc clusters list --region {region} --project {teamspace} --filter='status.state = RUNNING'" 
    second = "| awk '{if (NR!=1) {print $1 }}'"
    cmd1 = first + second
    #print(cmd1)
    a, b = subprocess.getstatusoutput(cmd1)
    running_cls = b.splitlines()
    #c_date_time = datetime.now()
    c_time_cmd='date -u +"%Y-%m-%dT%H:%M:%SZ"'
    a, c_date_time = subprocess.getstatusoutput(c_time_cmd)
    #print("Current Date", c_date_time)
    cd1 = (c_date_time.split("T"))
    cd2 = cd1[1].strip("Z")
    current_t = cd1[0]+" "+cd2
    current_t_obj = datetime.strptime(current_t, '%Y-%m-%d %H:%M:%S')
    print(f"Current Date & Time is :- {current_t_obj}")
    print()
    #print(cd)
    #print(running_cls)
    #print(type(running_cls))
    i = 1
    for c in running_cls:
        if ('phs' in c) or ('persistent' in c):
            pass
        else:
            first = f"gcloud dataproc clusters describe {c} --region {region} --project {teamspace}"
            second = " | grep stateStartTime | head -1"
            third = " | awk '{ print $2 }'"
            cmd = first + second + third
            #print(cmd)
            a, start_time = subprocess.getstatusoutput(cmd)
            #print(start_time)
            cd1 = (start_time.split("T"))
            d = cd1[0].strip("'")
            t1 = (cd1[1].split("."))[0]
            start_t = d + " " + t1
            start_t_obj = datetime.strptime(start_t, '%Y-%m-%d %H:%M:%S')
            
            time_diff_mins = (current_t_obj - start_t_obj)
            
            stat = f"[{i}] {c} Cluster is running since last :- {time_diff_mins}"
            i+=1
            all_cls.append(stat)
            print(stat)
            print()
            
    print(100 * "_")
    print()
