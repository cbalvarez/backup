import sys
import argparse
import time
import datetime
import MySQLdb as mdb
import os
import urllib
import glacierint
import tempfile
import gzip
import ntpath
import stat
import glacierint
import config
import config




#maybe another test
def get_last_version(con, country, directory, file):
	file_name = directory + "/" + ntpath.basename(file) 	
	cur = con.cursor()	
	cur.execute('SELECT file_name, vault_name, archive_id, max(last_modified) FROM backup_files bf, backup_catalog bc WHERE bf.backup_id = bc.id AND bc.country =%s AND bc.directory = %s AND file_name = %s' , (country, directory, file_name)
	data = cur.fetchone()
	if data == None:
		return None
	else:
		return data[0],data[1],data[2],data[3] 


def get_retrieve_job(con, archive_id):
	cur = con.cursor()
	cur.execute('SELECT date_started, job_id, backup_id, is_finished FROM retrieve_jobs WHERE archive_id = %s' 
	return None

def is_job_finished(job_id):
	return False;

def download(con, archive_id):
	return None

def log(msg):
	print msg

server, database, username, password, tempdir = config.readconfig()

con = mdb.connect(server,username,password,database)

parser = argparse.ArgumentParser()
parser.add_argument('-c','--country')
parser.add_argument('-d','--directory')
parser.add_argument('-f','--file')
args = parser.parse_args()

r = get_last_version(con, args.country, args.directory, args.file)
if (r == None):
	log("no such file-directory-country")
	return 

file_name, vault_name, archive_id, modified_date = r
job = get_retrieve_job(con, archive_id)
if (is_job_finished(job)):
	download(con,archive_id)
else:
	log("job id %s still not finished")


