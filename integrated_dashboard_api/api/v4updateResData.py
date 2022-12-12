#!/usr/bin/python3
import requests
import subprocess
import google.oauth2
from pprint import pprint
from datetime import datetime,timedelta
import os
import mysql.connector
import pymysql
import yaml
from pytz import timezone
import time

class Reservation:
    def __init__(self, project, reservation, zone):
        self.project = project
        self.reservation = reservation
        self.zone = zone

    def getToken(self):
        cmd1 = f"gcloud auth print-access-token"
        a, access_token = subprocess.getstatusoutput(cmd1)
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        #print(a,access_token)
        return headers
    def getFiles(self):
        #dir = "/home/dfs_app/test_scripts/res_data/shared_res/"
        dir = "/Users/vn51hqy/GIT/Prod_Test/scripts/reservation/data/"
        os.environ['TZ'] = 'US/Central'
        time.tzset()
        now = datetime.now()

        next_day  = datetime.now() + timedelta(days=1)

        ntemp = next_day.strftime("%Y-%m-%d %H:%M:%S")
        next_day_file = f"{ntemp.split()[0]}.txt"

        curr_t = now.strftime("%Y-%m-%d %H:%M:%S")
        curr_day_file = f"{curr_t.split()[0]}.txt"

        # print("Dir : ", dir)
        # print("curr_day_file : ",curr_day_file)
        # print("next_day_file : ",next_day_file)
        return dir, curr_day_file, next_day_file

    def getRowDataAPI(self,url,headers,p,z,r):
        try:
            os.environ['TZ'] = 'US/Central'
            time.tzset()
            now = time.time()
            seconds = int(now)
            
            
            response = requests.get(url, headers=headers)
            d1 = response.json()
            #print(d1)
            timestamp = d1.get('creationTimestamp')
            reservation_name = d1.get('name')
            machine_type = d1.get('specificReservation').get('instanceProperties').get('machineType')
            count = d1.get('specificReservation').get('count')
            in_use_count = d1.get('specificReservation').get('inUseCount')
            assured_count = d1.get('specificReservation').get('assuredCount')
            now = datetime.now()
            t_cst = datetime.fromtimestamp(seconds)
            
            row = f"{p},{t_cst},{timestamp},{reservation_name},{machine_type},{z},{count},{in_use_count},{assured_count},{p}"
            print(row)
            return row
        except:
            row = "Some Error"
            return row
    
    def writeDatatoFile(self, row, dir, curr_day_file):
        abs_path = dir+curr_day_file
        print(abs_path)
        if os.path.exists(abs_path):
            with open(abs_path,'a') as fp:
                fp.write(row)
                fp.write("\n")
                return f"Record Updated  to {abs_path}"
        else:
            with open(abs_path,'w') as fp:
                fp.write(row)
                fp.write("\n")
                return f"Record Updated  to {abs_path}"


    def writeDatatoDB_from_row(self,row):
        path = "/Users/vn51hqy/GIT/Prod_Test/scripts/reservation/conf.yml"
        #path = "/home/dfs_app/test_scripts/conf.yml"
        with open(path, "r") as fp:
            conf_file = yaml.safe_load(fp)

        host = conf_file["Dev_DB"]["host"]
        port = conf_file["Dev_DB"]["port"]
        user = conf_file["Dev_DB"]["user"]
        password = conf_file["Dev_DB"]["password"]
        database = conf_file["Dev_DB"]["database"]


        try:
            with pymysql.connect(host = host, port = int(port), user = user, password = password, database = database, autocommit=True) as mysqldb:
                #cur = db.execute("use walmart")
                #print ("MySQL connection successful")
                cursor = mysqldb.cursor()
                sql = """INSERT INTO dfs.DCA_RES_DATA_SHARED 
                (teams_space, curr_Time_CST, Res_timestamp, reservation_name, 
                machine_type, zne, res_cnt, in_use_count,assured_count) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                
                r1 = row.split(",")
                teams_space = r1[0]
                curr_Time_CST = r1[1]
                Res_timestamp = r1[2]
                reservation_name = r1[3]
                machine_type = r1[4]
                zne = r1[5]
                res_cnt = r1[6]
                in_use_count = r1[7]
                assured_count = r1[8]
                #project = r1[9]
                val = (teams_space, curr_Time_CST, Res_timestamp, reservation_name, machine_type, zne, res_cnt, in_use_count, assured_count)
                cursor.execute(sql, val)
                mysqldb.commit()
            return f"Data inserted to table "
        except Exception as error:
            return f"Error while connecting to MySQL :-  {error}"



if __name__ == '__main__':
    project = ["wmt-dca-dataproc-res-prod", 
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod",
    "wmt-dca-dataproc-res-prod"
    ]

    reservation = ["fr-dca-shared-e2s4--20221031000144-lyvxm74z-1",
    "fr-dca-shared-n1h32-20221031000144-l5ftz6hv-1",
    "fr-dca-shared-n1s16-20221031001701-m7outknc-2",
    "fr-dca-shared-n1s32-20221031003144-eri2e6hq-3",
    "fr-dca-shared-n1s8--20221031000143-u9u10q5v-1",
    "fr-dca-shared-n1s8--20221031000144-uj4ne7m6-1",
    "fr-dca-shared-n1s8--20221031001701-t6fkjanq-2",
    "fr-esc-shared-n1h16-20221031000143-0ug9lzvd-1",
    "fr-opd-shared-n1h16-20221031000143-29921d9v-1",
    "fr-opd-shared-n1s16-20221031000143-192xcz1r-1",
    "fr-sc-shared-e2s16--20221031000144-1f0q61ol-1",
    "fr-sc-shared-e2s32--20221031000144-28fzs45v-1",
    "fr-sc-shared-n1s16--20221031000143-268yjqsv-1",
    "fr-sc-shared-n1s32--20221031000144-cnh3hgpf-1",
    "fr-sc-shared-n1s4-p-20221031000143-7rhk5897-1",
    "od-cbbsegmentr"
    ]

    zone = ["us-central1-a",
    "us-central1-a",
    "us-central1-b",
    "us-central1-a",
    "us-central1-c",
    "us-central1-a",
    "us-central1-b",
    "us-central1-c",
    "us-central1-a",
    "us-central1-a",
    "us-central1-c",
    "us-central1-c",
    "us-central1-c",
    "us-central1-c",
    "us-central1-c",
    "us-central1-b"
    ]


    # 0 : - Intitalise the class
    res = Reservation(project,reservation,zone)

    # 1 :- Genrating Access Tocken
    headers = res.getToken()

    # 2 :- get file names
    dir, curr_day_file, next_day_file = res.getFiles()
    for p, r, z in zip(project, reservation, zone ):
       
        # 3 :- Making api call
        #url = 'https://compute.googleapis.com/compute/v1/projects/wmt-bfdms-crewprod/zones/us-central1-f/reservations/reservation-crewprod-001'
        url = f'https://compute.googleapis.com/compute/v1/projects/{p}/zones/{z}/reservations/{r}'
        print(url)
        
        row = res.getRowDataAPI(url,headers,p,z,r)

        #  4 :- Write data to new file each day
        info = res.writeDatatoFile(row, dir, curr_day_file)
        print(info)
        
        ## 5 :- Write data to dev DB
        info = res.writeDatatoDB_from_row(row)
        print(info)
        
        
