from boto.glacier.layer1 import Layer1
from boto.glacier.vault import Vault
from boto.glacier.concurrent import ConcurrentUploader
import sys
import os.path
import logging
import datetime
import ConfigParser

def log(s):
	print("%s %s" % (datetime.datetime.now(),s))

def read_config():
	Config = ConfigParser.ConfigParser()
	Config.read('backup.ini')
	aws_access_key = Config.get('glacier','aws_access_key')
	aws_secret_key = Config.get('glacier','aws_secret_key')
	return (aws_access_key, aws_secret_key)


aws_access_key, aws_secret_key = read_config()

class GlacierInt:
	def __init__(self):
		self.glacier_layer = Layer1(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

	def create_vault(self,vault_name):
		self.glacier_layer.create_vault(vault_name)
		log("vault created %s" % vault_name)

	def upload_file(self,vault_name, file_name):
		log("starting upload file %s %s" % (file_name,vault_name))
		uploader = ConcurrentUploader(self.glacier_layer, vault_name, part_size = 32 * 1024 * 1024 )
		archive_id = uploader.upload(file_name, "")
		log("upload finished! %s" % archive_id)
		return archive_id


