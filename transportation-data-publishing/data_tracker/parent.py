# This is a parent script that may used to call all data_tracker scripts. This
# parent script will handle:

# ------------------------------------------------------------------------------
# Import packages

import subprocess as sp

import _setpath
from config.secrets import *

from tdutils import emailutil
from tdutils import logutil

scriptname = "backup"

def getobject(scriptname): # collect objects number based on the name of scripts

	# define which objects the scripts will work on
	if funcname == "backup.py": 

		objects = ['object_87', 'object_93', 'object_77', 'object_53', 'object_96',
		 'object_83', 'object_95', 'object_21', 'object_14', 'object_109', 
		 'object_73', 'object_110', 'object_15', 'object_36', 'object_11', 
		 'object_107', 'object_115', 'object_116', 'object_117', 'object_67', 
		 'object_91', 'object_89', 'object_12', 'object_118', 'object_113', 
		 'object_98', 'object_102', 'object_71', 'object_84', 'object_13', 
		 'object_26', 'object_27', 'object_81', 'object_82', 'object_7', 
		 'object_42', 'object_43', 'object_45', 'object_75', 'object_58', 
		 'object_56', 'object_54', 'object_86', 'object_78', 'object_85', 
		 'object_104', 'object_106', 'object_31', 'object_101', 'object_74', 
		 'object_94', 'object_9', 'object_10', 'object_19', 'object_20', 
		 'object_24', 'object_57', 'object_59', 'object_65', 'object_68', 
		 'object_76', 'object_97', 'object_108', 'object_140', 'object_142', 
		 'object_143', 'object_141', 'object_149']

	return objects

#collect all other arguments other than object number

def getkwargs():	

	if funcname == "backup.py":

		kwarg = {"app_name": "data_tracker_prod"}

	# elif funcname == "esb_xml_gen.py": 

	# 	**kwarg = 

	return kwarg

# Define a function that run scripts, catches error, write logs and send alert email

def runscript(scriptname):

	bashcommand = "python" + " " + scriptname + ".py"

	try: 

		sp.run(bashcommand, shell = True).returncode

	except Exception as e:

		logger.error(str(e))
		emailutil.send_email(
			ALERTS_DISTRIBUTION, 
			'Data Bakup Exception', 
			str(e), EMAIL['user'], 
			EMAIL['password'])

		job.result('error', message=str(e))



if __name__ == "__main__":
	runscript("backup")



# def runscript(funcname, arg, **kwarg):

# 	subprocess()
	
# 	try:

# 		subprocess.run(funcname, shell = True).returncode

# 	except Exception as e: