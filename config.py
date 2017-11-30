import ConfigParser

#trestetesttestestest

def readconfig():
	Config = ConfigParser.ConfigParser() 
	Config.read('backup.ini') 
	server = Config.get('database','server') 
	username = Config.get('database','user') 
	password = Config.get('database','password') 
	database = Config.get('database','database') 
	tempdir = Config.get('temp','temp_dir') 
	return (server, database, username, password, tempdir)

