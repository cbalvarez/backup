mport sys
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


#just to test
class FileBackupResult:
	def __init__(self, id, name, last_modified, result):
		self.id = id
		self.name = name
		self.result = result
		self.last_modified = last_modified

	def __str__(self):
		return "aaa"

	def __repr__(self):
		return "{id: %s, name: %s, result:%s, last_modified: %s}" % (self.id, self.name, self.result, self.last_modified)



class FileData:
	def __init__(self, name, last_modified):
		self.name = name
		self.last_modified = last_modified
		
glacierint = glacierint.GlacierInt()


def list_directory(directory):
	r = []
	for root, dir, files in os.walk(directory):
		for name in files:
			filename = os.path.join(root,name)
			last_modified_date = None
			try:
				stinfo = os.stat(filename)
				if (stat.S_ISREG(stinfo.st_mode) != True):
					log("%s is not a regular file - not backing up" % name)	
					continue
				last_modified_date = datetime.datetime.fromtimestamp(os.stat(filename).st_mtime)
			except Exception:
				log("%s is a broken link. do not backup" % filename)
			if (last_modified_date != None):
				r.append(FileData(filename, last_modified_date.replace(microsecond = 0)))
	return r

def vault_name(country, directory, timestamp):
	t = time.strftime("%Y%m%d%H%M%S",timestamp)
	vault_name = "%s-%s-%s" % (country, directory.replace("/","-"), t)
	return vault_name

def create_vault(vault_name):
	log("creating vault %s" % vault_name)
	glacierint.create_vault(vault_name)


def compress_file(directory, file_name):
	output_file = directory + "/" + ntpath.basename(file_name) + '.gz'
	log("compressing %s into %s" % (file_name, output_file))
	f_in = open(file_name)
	f_out = gzip.open(output_file,'wb')
	while True:
		chunk = f_in.read(2000)
		if chunk:
			f_out.write(chunk)
		else:
			break
	f_out.close()
	f_in.close()
	return output_file


def perform_backup(file_list, temp_dir, vault):
	r = []
	for f in file_list:
		output_file = compress_file('/home/barto/tmp',f.name)
		id = glacierint.upload_file(vault, output_file)
		r.append(FileBackupResult(id, f.name, f.last_modified, 0))
	return ("backup finalized",r)


def store_catalog(con, timestamp, vault_name, country, directory, msg, status):
	cur = con.cursor()
	cur.execute( "INSERT INTO backup_catalog (vault_name, country, directory, backup_date, description, status) VALUES (%s,%s,%s,%s,%s,%s)" ,(vault_name, country, directory, timestamp, msg, status))
	return cur.lastrowid


def store_files(con, backup_id,files_to_backup):
	for f in files_to_backup:
		cur = con.cursor()
		t = f.last_modified.strftime("%Y-%m-%d %H:%M:%S")
		cur.execute("INSERT INTO backup_files (backup_id, archive_id, file_name, status, last_modified) VALUES (%s,%s,%s,%s,%s) ",(backup_id,f.id,f.name, f.result, t))
	


def store_backup_result(con, files_to_backup, vault_name, country, directory, timestamp,msg,status):
	t = time.strftime("%Y-%m-%d %H:%M:%S",timestamp)
	backup_id = store_catalog(con, t, vault_name, country, directory, msg, status)
	log('backup #%d ' % backup_id)
	store_files(con, backup_id, files_to_backup)
	con.commit()
	return None


def get_prev_backup_id(con, country, directory):
	cur = con.cursor()
	cur.execute('SELECT id, vault_name, country, directory, backup_date, status  FROM backup_catalog bc  WHERE bc.backup_date = ( SELECT max(backup_date) FROM backup_catalog bc2 WHERE country = %s AND directory = %s) AND country = %s AND directory = %s' , (country, directory, country, directory))
	data = cur.fetchone()
	if (data != None): 
		log("prev backup id: %s - date: %s" % (data[0],data[4]))
		return data[0] 
	else: 
		log("no backup id")
		return None
	

def get_backup_files(con, country, directory):
	cur = con.cursor()	
	data = cur.execute('SELECT a.archive_id, a.file_name, b.status, MAX(a.last_modified) FROM backup_files a, backup_catalog b where a.backup_id = b.id and b.country = %s and directory = %s and b.status = 0 group by a.archive_id', (country, directory))
	r = {} 
	for f in cur.fetchall():
		r[f[1]] = FileBackupResult(f[0], f[1], f[3], f[2]) 
	return r


def get_prev_backup(con, country, directory):
	id = get_prev_backup_id(con, country, directory)
	return get_backup_files(con, country, directory)


def get_modified_files(con, country, directory, files_to_backup):
	prev_files = get_prev_backup(con, country, directory)
	r = []
	for f in files_to_backup:
		prev_file = prev_files.get(f.name)
		if (prev_file == None):
			r.append(f)
			continue
		if (prev_file.last_modified < f.last_modified):
			r.append(f)
			continue
		log("file %s not modified" % f.name)
	return r




def log(msg):
	print("%s %s" % (datetime.datetime.now(), msg))


parser = argparse.ArgumentParser()
parser.add_argument('-c','--country')
parser.add_argument('-d','--directory')
args = parser.parse_args()

if (args.country == None or args.directory == None):
	print "usage: python backup --country=<country> --directory=<directory to be backed up>"
	exit(1)

timestamp = time.gmtime()

server, database, username, password, tempdir = config.readconfig()


con = mdb.connect(server,username,password,database)
vault_name = vault_name(args.country, args.directory, timestamp)
log("vault name for this backup: %s" % vault_name)
files_to_backup = list_directory(args.directory)
files_to_backup = get_modified_files(con, args.country, args.directory, files_to_backup)
if (len(files_to_backup) > 0):
	create_vault(vault_name)
	msg, file_result = perform_backup(files_to_backup, tempdir, vault_name)
	store_backup_result(con, file_result, vault_name, args.country, args.directory, timestamp,msg,0) 
else:
	log("no backup performed - no news")


