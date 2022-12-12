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

print("Started v5 to modularise function")

def input_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", "--JOBS", "-j", "-J", type=str, required=True)
    args = parser.parse_args()
    jobs = (args.jobs).upper()
    print(jobs)
    pref_list = ["JOBS", "jobs", "JB", "jb", "MDSE"]
    if not args.jobs.startswith(tuple(pref_list)):
        print(100 * "_")
        print()
        print("Please verify JOBS name. Please provide JOBS name only not workflow")
        print(100 * "_")
        print()
        sys.exit(1)
    else:
        path = "/Users/vn51hqy/GIT/Prod_Test/scripts/pattern.yml"
        edge_node_yml = "/Users/vn51hqy/GIT/Prod_Test/scripts/edge_node.yml"
        #path = "/root/script/pattern.yml"
        #edge_node_yml = "/root/script/edge_node.yml"
        with open(path, "r") as fp:
            conf_file = yaml.safe_load(fp)
        with open(edge_node_yml, "r") as fp1:
            edge_node_file = yaml.safe_load(fp1)
    
        print(100 * "_")
        print()
        print("Analysing Job :", jobs)
        # v_config_file='/Users/vn51hqy/GIT/Prod_Test/conf/config_prd.yml'
        # a = file['queries']['q1']
        db_conn_file = conf_file["file_path"]["db_config_file"]
        config_file_section = "Oracle_9010"
        connection = connectDB.oracledbconfig(db_conn_file, config_file_section)
        cursor1 = connection.cursor()
        return jobs, cursor1, conf_file, edge_node_file

def executeQuery(jobs,cursor1,conf_file):
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
    return query_output, svc_id

def getRunid(query_output):
    if (len(query_output)) > 0:
        for rows in query_output:
            run_id = rows[6]
    else:
        print("Run Id not Found")
    return run_id

def write_logs_into_file(query_output):
    with open("log1.txt", "w") as f:
        f.write("")
    for rows in query_output:
        log_content = rows[5]
        content = str(log_content)
        # writting logs into log1.txt file
        with open("log1.txt", "a") as f:
            f.write(content)

def statusPrint(query_output, svc_id, edge_node_file):
    status = ""
    
    for rows in query_output:
        #log_content = rows[5]
        #content = str(log_content)
        last_m = rows[8]
        ert = rows[7]
        rt = rows[2]
        status = rows[1]
        runid = rows[6]
        enode = rows[9]
        ert_m = str(datetime.timedelta(seconds=ert))
        rt_m = str(datetime.timedelta(seconds=rt))
    if os.path.getsize("log1.txt") > 0:
        print("Current status : ", status)
        print("Runtime        : ", rt_m)
        print("Estimated RT   : ", ert_m)
        print("Last Modified  : ", last_m)
        print("Run ID         : ", runid)
        print("SVC ID         : ", svc_id)
        for k, v in edge_node_file["edgeNode"].items():
            #print(k, v)
            if k == enode:
                edge_node_ip = v
        if enode.startswith("AMEN"):
            print(f"Edge Node      :  {enode}  IP Address is : {edge_node_ip}")
            if enode.startswith("AMEN"):
                print()
                print("Please log in into AMEN-ADHOC-Edge-Node :- 10.22.167.38   or DFS IEN node :- 10.22.144.174")
                edge_node_ip = '10.22.144.174'
        else:
            print(f"Edge Node      :  {enode}  IP Address is : {edge_node_ip}")
    else:
        print("Logs are not availabes here, please verify when jobs last executed, its fetch logs from past 1 day.")
        print(100 * "_")
        print()  
    
    return status, edge_node_ip, rt, ert

def rt_suggestion1(rt, ert, status): 
    print(100 * "_")
    print()
    i_runl = int(ert) / 2
    i_runh = int(ert) * 2
    if status == "FAILED":
        if rt < i_runl or rt > i_runh:
            print("Suggested to restart the job based on RT, Please refer below log output causing failuar.")
        else:
            print("Suggested :- Please take a call based on logs.")
    else:
        print("Last Execution of the job has completed Successfully, please verify.")
        
    
def readLogFile():
    with open("log1.txt", "r") as fp:
        error_file_content = fp.read()
    return error_file_content

def displayErrors(error_file_content, conf_file):
    
    m1=[]; m2 = []; m3 = []
    error_logs = []
    display_unique_logs = []
    # Start
    for k, v in conf_file["typeoferror"].items():
        m1.append(k)
        v_keys = list(v.keys())
        s_keys = str(v_keys[0])
        m2.append(s_keys)
        v_values = list(v.values())
        s_values = str(v_values[0])
        m3.append(s_values)
    for i in m1:
        pat_yml = conf_file["typeoferror"][i]["p1"]
        pat_comp = re.compile(pat_yml, re.IGNORECASE)
        pat_find = pat_comp.findall(error_file_content)
        
        for match in pat_find:
            if 'WARN' in match:
                pass
            elif 'INFO' in match:
                pass
            else:
                #print(pat_comp)
                error_logs.append(match)

    for i in error_logs:
        if i not in display_unique_logs:
            display_unique_logs.append(i)
  
    if len(display_unique_logs) > 1:
        print(100 * "_")
        print()
        for a in display_unique_logs:
            print(a)
            print()
         
    else:
        print("Errors not found in log file")
        
            
    return display_unique_logs

def dpass_suggestions(dis_sugg, error_logs):
    if 'Welcome to the Automic Managed DPAAS Edgenode' in error_logs:
        if len(dis_sugg) > 0:
            for i in dis_sugg:
                if('$$$gcp' in i):
                    for e in error_logs:
                        if e.startswith("tracking URL:"):
                            e1 = e.partition("-m.c")
                            cluster_name = ((e1[0]).strip("tracking URL: https://"))
                            e2 = (e1[2]).partition(":")
                            ts_name = ((e2[0]).strip(".internal"))
                    if ( len(cluster_name)>1):
                        print(100 * "_")
                        print()
                        print(i.replace("$$$gcp", ts_name ).replace("$$$CN", cluster_name))
                        print()
    

def suggestions(error_logs, conf_file):
  
    m1=[]; m2 = []; m3 = []
    suggested = []
    usuggested = []
    sugg2 = ""
    for k, v in conf_file["suggestions"].items():
        m1.append(k)
        v_keys = list(v.keys())
        s_keys = str(v_keys[0])
        m2.append(s_keys)
        v_values = list(v.values())
        s_values = str(v_values[0])
        m3.append(s_values)
    for i in m1:
        e1 = conf_file["suggestions"][i]["e1"]
        for e in error_logs:
            if e1 in e:
                sugg2 = conf_file["suggestions"][i]["s1"]
                suggested.append(sugg2)
                #print()
                #print("Suggestion is basised on Error :- \n ",e)
                #print()
        if any(e1 in s for s in error_logs):
            pass
             
    for i in suggested:
        if i not in usuggested:
            usuggested.append(i)
    #print(usuggested)
    return usuggested

def process_suggestion(dis_sugg, error_logs):
    if len(dis_sugg) > 1:
        print(100 * "_")
        print()
    
    for i in dis_sugg:
        if('$$$gcp' not in i):
            print(i)
    
    #print (len(dis_sugg))
    if len(dis_sugg) >= 1:
        print(100 * "_")
        print() 
    

def fetchVeriableValues(cursor1, run_id, process_tab_con_list):
    #print(process_tab_con_list)
    pt_dict = {}
    for i in process_tab_con_list:
        if "=" in i and 'if' not in i and 'SA_AUTH_FILE' not in i:
            i1 = i.strip("export ")
            a = i1.split("=")
            pt_dict[a[0]] = a[1].strip("''")    
    #print(pt_dict)
    #print("Fetching Variabl Values")
    variable_values = []
    dict_variable_values = {}
    test_query = f"SELECT RT_MSGINSERT FROM uc4.RT o2  WHERE RT_AH_IDNR = '{run_id}'"
    #print(test_query, sep="\n")
    cursor1.execute(test_query)
    rows = cursor1.fetchall()
    with open("object_values.txt", "w") as f:
        for x, in rows:
            if x != None:
                if str(x).startswith("&"):
                    f.write("%s\n" % x)
                    variable_values.append(str(x))
    #print(variable_values, type(variable_values))
    for v in variable_values:
            v1 = (v.split("|"))
        #print(v1[:2])
            if v1[1] == '':
                dict_variable_values[v1[0]] = 'NA'
            else:
                dict_variable_values[v1[0]] = v1[1]
    #print(dict_variable_values)
    #print(variable_values)
    for k, v in pt_dict.items():
        dict_variable_values[k] = v
    #print(dict_variable_values)
    return dict_variable_values


def process_content(cursor1, jobs, run_id):
    #print("process content fetch start")
    process_tab_con_list = []
    source = []
    export = []
    URL = []
    sh = []
    cd = []
    test_query = f"SELECT OT_CONTENT FROM uc4.OT WHERE OT_OH_IDNR = (SELECT OH_IDNR FROM uc4.OH o3 WHERE OH_NAME = '{jobs}')"
    cursor1.execute(test_query)
    rows = cursor1.fetchall()
    for x, in rows:
        if str(x) == "None":
            x = "\n"
        process_tab_con_list.append(str(x))
    with open("process_tab.txt", "w") as f:
        for line in process_tab_con_list:
            f.write("%s\n" % line)
    for p in process_tab_con_list:
        if p.startswith("source"):
            if p != 'source ~/.profile':
                source.append(p)
        if p.startswith("export"):
            export.append(p)
        if p.startswith("URL"):
            URL.append(p)
        if p.startswith("sh"):
            sh.append(p)
        if p.startswith("cd"):
            cd.append(p)
    return source, export, URL, sh, cd, process_tab_con_list

def replaceSource(sources, dict_variable_values):
    
    for source in sources:
        s1 = source.strip("source .sh")
        #print(s1)
        s2 = s1.split("/")
        for s in s2:
            if s.startswith("&"):
                i = s2.index(s)
                s2[i] = dict_variable_values[s]
            if s.startswith("ien"):
                s3 = s.split("-")
                for ss in s3:
                    if ss.startswith("&"):
                        i = s3.index(ss)
                        s3[i] = dict_variable_values[ss]

    ien = ("-".join(s3))
    s2[-1] = ien
    r_source = "source "+ ("/".join(s2)) + ".sh"
    #print(r_source)
    return r_source


def replaceSh(sh, dict_variable_values):
    print("replace Sh")
    print(sh)

def replaceVariable(s, dict_variable_values,process_tab_con_list):
    #print(s)
    #print(dict_variable_values)
    try:
        s1 = s.split()
        for var in s1:
            if var.startswith("&"):
                i = s1.index(var)
                #print(var, i)
                s1[i] = dict_variable_values[var]  
                #s1[i] = dict_variable_values.get[var]  
        r_string = " ".join(s1)
        print(s)
        print(r_string)
        return r_string
    except:
        pass
def shellScriptCheck(cd, sh, dict_variable_values, process_tab_con_list):
    try:
        #print("Shell Script Checking Started")
        #print("pass var :- ", sh)
        #print(cd)
        #print(process_tab_con_list)
        absulte_path = ''
        sh_script = ''
        base_path = ''
        if len(sh) > 0:
            for s in sh:
                r_string = replaceVariable(s, dict_variable_values,process_tab_con_list)
                #print("r_string : ", r_string)
            for rs in (r_string.split()):
                if rs.endswith(".sh"):
                    sh_script = rs 

        if len(cd) > 0:        
            for c in cd:  
                r_string = replaceVariable(c, dict_variable_values,process_tab_con_list)
                #print("r_string c : ", r_string) 
            for cd1 in (r_string.split()):
                #print(cd1)
                if cd1.startswith("cd "):
                    print("ok")
                else:
                    base_path = cd1.strip()
                #print("base_path : ", base_path)
                    
            
        if sh_script.startswith("/u") or sh_script.startswith("/edge_data"):
            absulte_path = sh_script
        else:
            if len(base_path) > 0 or len(sh_script) > 0:
                if base_path.endswith("/") or sh_script.startswith("/"):
                    absulte_path = base_path + sh_script
                else:
                    absulte_path = base_path + "/" + sh_script
        
        #print("Absulute path : ",absulte_path)
        return absulte_path
    except:
        pass   

def lastModified(absulte_path, edge_node_ip,svc_id):
    try:
        #print("Last Modification Start")
        #print(edge_node_ip)
        #print(absulte_path)
        if len(absulte_path) > 0:
            #print("Last Mdification Block Started")
            ip = edge_node_ip
            port = 22
            username = 'vn51hqy'
            password = 'PSL@wal0912'
            cmd1 = f'stat {absulte_path} | grep Modify'
            #print(svc_id)
            cmd = cmd1.replace('$USER', svc_id.lower())
            #print(cmd)
            try:
                ssh=paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(ip,port,username,password)
                try:
                    stdin,stdout,stderr=ssh.exec_command(cmd)
                except Exception as e:
                    print(e)
                last_modified = (stdout.read().decode())
                if len(last_modified) > 0:
                    print ("Shell Script : ", absulte_path)
                    print(last_modified)
                    print(100 * "_")
                    print()
                del ssh, stdin, stdout, stderr
            except:
                print()
                print("You Dont have access on this node")
                print(100 * "_")
                print()
    except:
        pass


def sourceStatementCheeck(source, dict_variable_values, process_tab_con_list):
    #print(dict_variable_values)
    source_state = ''
    r_source_state = ''
    s_ien = ''
    for s in source:
        source_state = s
    s1 = source_state.strip(" source .sh")
    s2 = s1.split("/")
    for s in s2:
        if s.startswith("&"):
            i = s2.index(s)
            for k, v in dict_variable_values.items():
                if k == s:
                    s2[i] = v
    for s in s2:
        if s.startswith("ien"):
            i = s2.index(s)
            s1 = s.split("-")
            for s in s1:
                if s.startswith("&"):
                    i = s1.index(s)
                    for k, v in dict_variable_values.items():
                        if k == s:
                            s1[i] = v
                            
            s_ien = ("-".join(s1))
    s2[-1] = s_ien
    r_source_state = "source " + ("/".join(s2)) + ".sh"
    if len(source_state) > 0:
        print(source_state) 
        print(r_source_state)
        print(100 * "_")
        print()


#Accepting command line input & connecting to DB
jobs, cursor1, conf_file, edge_node_file = input_args()
#Execute Intial Query
query_output, svc_id = executeQuery(jobs,cursor1,conf_file)

#write log to log1.txt
write_logs_into_file(query_output)
#printing status Block
status, edge_node_ip, rt, ert = statusPrint(query_output, svc_id, edge_node_file)
#printing RT Suggestion Block
rt_suggestion1(rt, ert, status)

status = 'FAILED'
if status != "SUCCESS":

    log_file_content = readLogFile()
    #Printing Error Log Content
    error_logs = displayErrors(log_file_content, conf_file)
    # only fetching suggestion not printing
    dis_sugg = suggestions(error_logs, conf_file)
    # Printing GCP Project & Cluster Name
    dpass_suggestions(dis_sugg, error_logs)
    # Printing Suggestion Block
    process_suggestion(dis_sugg, error_logs)
    #Fetch Run_id from outout
    run_id = getRunid(query_output)
    
    #fetch source, export, URL, sh, cd statement from process tab
    source, export, URL, sh, cd, process_tab_con_list = process_content(cursor1, jobs, run_id)

    #Fetch all variable values in Dictionary
    dict_variable_values = fetchVeriableValues(cursor1, run_id, process_tab_con_list)

    # To get absulute path os schell script
    absulte_path = shellScriptCheck(cd, sh, dict_variable_values, process_tab_con_list)
    # To check when script was last modified:
    lastModified(absulte_path, edge_node_ip,svc_id)
    
    # To check if source statement in script
    sourceStatementCheeck(source, dict_variable_values, process_tab_con_list)

   


    

