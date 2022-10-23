# Returns configvariable as dictionary to access configuration variables

import sys
import os
from pathlib import Path

from app.config.decrypt import decrypt

key = sys.argv[1]
env = sys.argv[2]

class Parse():

	def parse_configuration_file(self):
	    
	    filename = os.getcwd()+f"\\app\\config\\application-{env}-properties.py"
	    config= open(filename).read()
	    config= eval(config)
	    return config

	def parse_variables(self, config):
	    
	    config['PASSWORD'] = decrypt(key, config['PASSWORD'])
	    config['JDBC'] = os.getcwd() + config['JDBC']
	    return config

	def parse(self):

		config = self.parse_configuration_file()
		config = self.parse_variables(config)
		return config

parse = Parse()
config= parse.parse()



