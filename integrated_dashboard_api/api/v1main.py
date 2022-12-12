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

'''
log ananalysis for ops dashobard.
requuired jobs as a input :- done
[1] connect to oracle db :- done
[2] get prilimary info :- Done
    Current status :  FAILED
    Last Modified  :  2019-08-02 09:31:12
    Estimated RT   :  0:06:55
    Runtime        :  0:12:11
    Run ID         :  568689703
    SVC ID         :  SVCPRICWB
     
    shell script is present or not, when it is modified  :- pending
    variabled used with values:- pending
    code block :- Done
    latest log :- Done

    
[3] logAnalysis :- Pending

'''

class LogAnalysis:
    def __init__(self, conf_file):
        self.conf_file = conf_file
    
    def input_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--jobs", "--JOBS", "-j", "-J", type=str, required=True)
        args = parser.parse_args()
        jobs = (args.jobs).upper()
        pref_list = ["JOBS", "jobs", "JB", "jb", "MDSE"]
        if not args.jobs.startswith(tuple(pref_list)):
            print(100 * "_")
            print()
            print("Please verify JOBS name. Please provide JOBS name only not workflow")
            print(100 * "_")
            print()
            sys.exit(1)
        else:
            return jobs
    
    def connnectdb(self):
        os.system('clear')
        print()
        print(100 * "_")
        print()
        db_conn_file = self.conf_file["file_path"]["db_config_file"]
        config_file_section = "Oracle_9010"
        connection = connectDB.oracledbconfig(db_conn_file, config_file_section)
        cursor1 = connection.cursor()
        #print(cursor1)
        return cursor1
    def executeQuery(self, jobs,cursor1,conf_file):
        test_query_yml = conf_file["queries"]["q1"]
        test_query = f"{test_query_yml}".replace("$$$JOB", jobs)
        try:
            cursor1.execute(test_query)
            query_output = cursor1.fetchall()
            if (len(query_output)) > 0:
                for rows in query_output:
                    run_id = rows[6]
                if (len(run_id)) > 0:
                    test_query1 = f"SELECT AH_LOGINDSTO FROM uc4.JBA j , uc4.AH a WHERE j.JBA_OH_IDNR = a.AH_oh_IDNR AND a.AH_IDNR = '{run_id}'"
                    cursor1.execute(test_query1)
                    svc_id = cursor1.fetchall()
                    sv = str(svc_id[0])
                    sv01=((sv.strip("()'',")).split("."))
                    svc_id = (sv01[-1])
                    if len(svc_id) > 0 :
                        pass
                    else:
                        print("SVC ID not found")  
            else:      
                print("Failed to fetch details, please verify jobs name.")
                sys.exit(1)
        except Exception:
            print("Failed to fetch details, please verify jobs name.")
            sys.exit(2)
        print(100 * "_")
        print()
        return query_output, svc_id, run_id


    def write_logs_into_file(self,query_output):
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/log1.txt'
        with open(path, "w") as f:
            f.write("")
        for rows in query_output:
            log_content = rows[5]
            content = str(log_content)
            # writting logs into log1.txt file
            with open(path, "a") as f:
                f.write(content)
        print("Logs has been updated to log File : - ", path)

    def write_code_into_file(self, jobs):
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/code1.txt'
        test_query_yml = conf_file["queries"]["q2"]
        test_query = f"{test_query_yml}".replace("$$$JOB", jobs)
        try:
            cursor1.execute(test_query)
            query_output = cursor1.fetchall()
        except Exception:
            print("Failed to fetch details, please verify jobs name.")
        with open(path, "w") as f:
            f.write("")
        for rows in query_output:
            code_content = rows[3]
            content = str(code_content)
            with open(path, "a") as f:
                f.write(content)
                f.write("\n")
    
        print("Process Tab Contains has been update code file  :- ", path)


    def getDqRuleStaus(self):
        pat_str = '.*rules failed!+.*'
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/log1.txt'
        with open(path) as file:
            content = file.read()
        pattern = re.compile(pat_str, re.IGNORECASE)
        result = pattern.findall(content)
        if result:
            print("DQ Rule Status :- ")
            for row in result:
                print(row)
            print(100 * "_")
            print()


    def getErrors(self):
        pat_str = '.*Error+.*'
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/log1.txt'
        with open(path) as file:
            content = file.read()
        pattern = re.compile(pat_str, re.IGNORECASE)
        result = pattern.findall(content)
        if result:
            print("Error Msg Block :- ")
            for i in result:
                print(i)
            print(100 * "_")
            print()

    def getFileNotAvailable(self):
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/log1.txt'
        pat_str = '.*is not available+.*'
        with open(path) as file:
            content = file.read()
        pattern = re.compile(pat_str, re.IGNORECASE)
        result = pattern.findall(content)
        if result:
            for i in result:
                print(i)
            print(100 * "_")
            print()


    def getDiagnostics(self):
        dia_comm = ['diagnostics: N/A','diagnostics: AM container is launched, waiting for AM container to Register with RM']
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/log1.txt'
        pat_str = 'diagnostics+.*'
        with open(path) as file:
            content = file.read()
        pattern = re.compile(pat_str, re.IGNORECASE)
        result = pattern.findall(content)
        if result:
            for r in result:
                if r not in dia_comm:
                    print(r)
            print(100 * "_")
            print()
        '''
        else:
            print("Diagnostics is not fount in the content")
        '''

    def getClusterDetails(self, vari_dict):
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/log1.txt'
        pat_str = 'tracking URL+.*'
        row1 = ''
        with open(path) as file:
            content = file.read()
        if 'Welcome to the Automic Managed DPAAS Edgenode' in content:
            pattern = re.compile(pat_str, re.IGNORECASE)
            result = pattern.findall(content)
            if result:
                tracking_url = result[0]
                trac_split = tracking_url.partition("-m.c.")
                first = trac_split[0]
                last = trac_split[-1]
                clust_name  = (first.split("//"))[-1]
                teams_space = last.split(".")[0]
                print(f"Teams Space Name is :- {teams_space}")
                print(f"Cluster Name is :- {clust_name}")
                print()
                print(f"{tracking_url}")
                print(100 * "_")
                print()
            
            else:
                print("Tracking URL is not fount in the content")
                print(100 * "_")
                print()
        else:
            pattern = re.compile(pat_str, re.IGNORECASE)
            result = pattern.findall(content)
            if result:
                for row in result:
                    if  '-m.c.wmt-bfdms-' in row:
                        trac_split = row.partition("-m.c.")
                        first = trac_split[0]
                        last = trac_split[-1]
                        clust_name  = (first.split("//"))[-1]
                        teams_space = last.split(".")[0]
                        print(f"Teams Space Name is :- {teams_space}")
                        print(f"Cluster Name is :- {clust_name}")
                        print()

                    if row1 != row:
                        print(row)
                        row1 = row
                print(100 * "_")
                print()

            
            
    

    def statusPrint(self, query_output, svc_id):
        status = ""
        edge_node_yml = "/Users/vn51hqy/GIT/Prod_Test/scripts/edge_node.yml"
        for rows in query_output:
            #log_content = rows[5]
            #content = str(log_content)
            last_m = rows[8]
            ert = rows[7]
            rt = rows[2]
            status = rows[1]
            runid = rows[6]
            edge_node = rows[9]
            with open(edge_node_yml, "r") as fp1:
                edge_node_file = yaml.safe_load(fp1)
            edge_node_ip = edge_node_file.get("edgeNode").get(edge_node)
            ert_m = str(datetime.timedelta(seconds=ert))
            rt_m = str(datetime.timedelta(seconds=rt))
        if os.path.getsize("log1.txt") > 0:
            print("Current status : ", status)
            print("Last Modified  : ", last_m)
            print("Estimated RT   : ", ert_m)
            print("Runtime        : ", rt_m)
            print("Run ID         : ", runid)
            print("SVC ID         : ", svc_id)
            print("Edge Node      :", edge_node)
            print("Edge Node IP   :", edge_node_ip)
        print(100 * "_")
        print()

    def getVariableValues(self,cursor1,run_id):
        path  = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/variables.txt'
        test_query_yml = conf_file["queries"]["q3"]
        test_query = f"{test_query_yml}".replace("$$$RUNID", run_id)
        try:
            cursor1.execute(test_query)
            query_output = cursor1.fetchall()
        except Exception:
            print("Failed to fetch details, please verify jobs name.")
        with open(path, "w") as f:
            f.write("")
        for rows in query_output:
            code_content = rows[0]
            content = str(code_content)
            with open(path, "a") as f:
                f.write(content)
                f.write("\n")
        print("variables value updated to file :-  ", path)
        

    def getVariableDict(self):
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/variables.txt'
        vari_dict = {}
        with open(path,'r') as file:
            contet = file.read()
        line = contet.split("\n")
        for l in line:
            if "|" in l:
                pair = (l.split("|"))
                if len(pair) >= 2:
                    if pair[0] not in vari_dict:
                        vari_dict[pair[0]] = pair[1]
            if ":" in l:
                pair = l.split(":")
                if len(pair) >= 2:
                    if pair[0] not in vari_dict:
                        vari_dict[pair[0]] = pair[1]
        #print(vari_dict)
        return(vari_dict)
    
    def  replacedCodeinfile(self, vari_dict):
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/code1.txt'
        path1 = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/replaced_code1.txt'
        with open(path,'r') as file:
            contet = file.read()
        lines = contet.split("\n")
        with open(path1, "w") as f:
            f.write("")
        with open(path1,'a') as fp:
            for line in lines:
                rep_value = re.sub("".join(vari_dict.keys()), lambda match:vari_dict[match.string[match.start():match.end()]],line)
                fp.write(rep_value)
                fp.write("\n")
        print("Code Block is replaced with current variable values in the file :- " , path1)
        print(100 * "_")
        print()
        
        
    
    def getShellScripts(self,vari_dict):
        shells_scripts = []
        replaced_scripts = []
        path = '/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/code1.txt'
        with open(path,'r') as file:
            contet = file.read()
        lines = contet.split("\n")
        
        source_commands = getSourceCommands(lines)
        export_commands = getExportCommands(lines)
        kinit_c = getkinitCommands(lines)
        shell_script = getShellScript(lines)
        beeline_c = beelineCommands(lines)

        print("Commands :- ")

        if len(shell_script)  >  0:
            replaced_scripts = replaceValues(vari_dict, shell_script)
            print("Shell Scripts :-  ")
            for i in shell_script:
                print(i)
            print()
            print("Shell Scripts with replaced values :- ")
            for i in replaced_scripts:
                print(i)
            print(100 * "_")
            print()

            return shells_scripts, replaced_scripts

        else:
            shells_scripts =  "NA"
            replaced_scripts = "NA"
            print("Shell Script is not present in code block")
            print(100 * "_")
            print()
            return shells_scripts, replaced_scripts
            

    

def getSourceCommands(lines):
        source_commands = []
        for line in lines:
            if (('source' in line)  or ('!source' in line)):
                source_commands.append(line)
        #print(source_commands)
        return source_commands

def getExportCommands(lines):
    export_commands = []
    for line in lines:
        if (('export' in line)  or ('!export' in line)):
            export_commands.append(line)
    #print(export_commands)
    return export_commands


def getkinitCommands(lines):
    kinit_c = []
    for line in lines:
        if (('kinit' in line)  or ('!kinit' in line)):
            kinit_c.append(line)
    #print(kinit_c)
    return kinit_c

    
def getShellScript(lines):
    shell_script = []
    for line in lines:
        if (('.sh' in line)  or ('sh ' in line)):
            shell_script.append(line)
    #print(shell_script)
    return shell_script


def beelineCommands(lines):
    beeline_c = []
    for line in lines:
        if (('beeline' in line)  or ('!beeline' in line)):
            beeline_c.append(line)
    #print(beeline_c)
    return beeline_c

def replaceValues(vari_dict, itrator):
        replaced_v = []
        for i in itrator:
            rep_value = re.sub("|".join(vari_dict.keys()), lambda match:vari_dict[match.string[match.start():match.end()]],i)
            replaced_v.append(rep_value)
        return replaced_v

    
    
if __name__ == "__main__":
    ## Config File Path
    conf_file_path = "/Users/vn51hqy/GIT/Prod_Test/scripts/log_analysis_ops/config.yml"
    with open(conf_file_path, "r") as fp:
            conf_file = yaml.safe_load(fp)
    
    
    ##Inntitalasie the class
    log = LogAnalysis(conf_file)
    ##tacking jobs as input & retuen jobs
    jobs = log.input_args()
    ## connecting to db return cursor object
    cursor1 = log.connnectdb()
    ## Executimg main query
    query_output, svc_id, run_id = log.executeQuery(jobs,cursor1,conf_file)
    
    
    ##writing log into file
    log.write_logs_into_file(query_output)
    ##writing code into file
    log.write_code_into_file(jobs)
    ##writing variable values into file
    log.getVariableValues(cursor1,run_id)

   

    ## convert variables into key value paid
    vari_dict = log.getVariableDict()
    ## replace code with variables & write into replaced_code1.txt
    log.replacedCodeinfile(vari_dict)
     ## printing Status of the jobs
    log.statusPrint(query_output, svc_id)
    ##only printing cluster & teams Space Name
    log.getClusterDetails(vari_dict)
    ##find out  shell script & repalce with variables & display.
    shells_scripts, replaced_scripts = log.getShellScripts(vari_dict)



    ##print file status
    log.getFileNotAvailable()
     ##Print Diagnostic only
    log.getErrors()
     ##Print Diagnostic only
    log.getDiagnostics()
    ##Print DQ rule Status
    log.getDqRuleStaus()



    #Prining to validate data
    #print(cursor)
    #print (jobs) 
    #print(query_output)
    #print(svc_id)

    

    
