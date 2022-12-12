import pymysql
import cx_Oracle
import yaml
from cryptography.fernet import Fernet
from configparser import ConfigParser
import mysql.connector

oracledb=None
config_path="/tmp/properties.conf"
def mysqlconnectdb(host,port,user,password,database):
    #conn = None
    try:
        with pymysql.connect(host = host, port = int(port), user = user, password = password, database = database, autocommit=True) as mysqldb:
            #cur = db.execute("use walmart")
            print ("MySQL connection successful")
            return mysqldb
               
            
    except Exception as error:
        print("Error while connecting to MySQL",error)
        
        
def oracleconnectdb(host,port,user,password,service_name,encoding):
    global oracledb
    try:
        #dsn_tns = cx_Oracle.makedsn('tstr600528.wal-mart.com', '1526', service_name='GDTRTST_SERVICE')
        #with cx_Oracle.makedsn(host = host, port = int(port), service_name = service_name) as dsn_tns:
        print("inside connectdb")
        dsn_tns = cx_Oracle.makedsn(host = host, port = port, service_name = service_name)
        #print(dsn_tns)
        #with cx_Oracle.connect(dsn = dsn_tns, user = user, password = password, encoding = encoding) as oracledb:
        oracledb=cx_Oracle.connect(user,password,dsn_tns,encoding="UTF-8", nencoding="UTF-8");
        print("Oracle connection successful", oracledb.version)
        #return oracledb

    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle",error)


def oracledbconfig(config_path,config_section):
    try:
        #print("inside db conn")
        #print(config_path)
        #print(config_section)
        conf = yaml.safe_load(open(config_path))
        
        v_host = conf[config_section]['host']
        v_port = conf[config_section]['port']
        v_database = conf[config_section]['database']
        v_user = conf[config_section]['user']
        v_keypath = conf[config_section]['keypath']
        v_password_enc = conf[config_section]['password']
        #print(v_host)
        #print(v_port)
        #print(v_user)
        #print(v_keypath)
        #print(v_password_enc)
        with open(v_keypath,"rb") as fpass:
            key = fpass.read()
            key = key.strip()
        f = Fernet(key)
        v_password_byte = f.decrypt(v_password_enc.encode('utf-8'))
        v_password = v_password_byte .decode()   
        #print(v_password)
        #v_key_byte = v_key.encode()
	    #key = Fernet.generate_key()
        #f = Fernet(v_key_byte)
        #v_password_enc = conf[config_section]['password']
        #v_password_enc_byt = v_password_enc.encode()
        #v_password = f.decrypt(v_password_enc_byt)
        v_encoding = conf[config_section]['encoding']
        #print(v_encoding)
        oracleconnectdb(v_host,v_port,v_user,v_password,v_database,v_encoding)
        return oracledb
    
    except Exception as error:
        print("Error while reading the configuration file",error)        
            
            
def mysqldbconfig(config_file_path,config_section):
    try:

        conf = yaml.safe_load(open(config_file_path))
        v_host = conf[config_section]['host']
        v_port = conf[config_section]['port']
        v_database = conf[config_section]['database']
        v_user = conf[config_section]['user']
        v_keypath = conf[config_section]['keypath']
        v_password_enc = conf[config_section]['password']
        
        with open(v_keypath,"rb") as fpass:
            key = fpass.read()
            key = key.strip()
        f = Fernet(key)
        v_password_byte = f.decrypt(v_password_enc.encode('utf-8'))
        v_password = v_password_byte .decode()   

        return mysqlconnectdb(v_host,v_port,v_user,v_password,v_database)
    
    except Exception as error:
        print("Error while reading the configuration file",error)

def getMySQLConnection(config_file_path,config_section):
    
        config_file_path=config_file_path
        config_section=config_section
        conf = yaml.safe_load(open(config_file_path))
        v_host = conf[config_section]['host']
        v_database = conf[config_section]['database']
        v_user = conf[config_section]['user']
        v_keypath = conf[config_section]['keypath']
        v_password_enc = conf[config_section]['password']
        #v_key_byte = v_key.encode()
	#key = Fernet.generate_key()

        with open(v_keypath,"rb") as fpass:
            key = fpass.read()
            key = key.strip()
        f = Fernet(key)
        v_password_byte = f.decrypt(v_password_enc.encode('utf-8'))
        v_password = v_password_byte .decode()   

        connection = mysql.connector.connect(host=v_host,
                                                database=v_database,
                                                user=v_user,
                                                password=v_password,
                                                autocommit=True,
                                                charset='utf8',
                                                use_unicode=True)
	
        return connection

#if __name__ == "__main__" : 
#config(config_path)
    #connectdb(host,port,user,password,database)
    
