# ------------------------------- Information ------------------------------- #
# Author:       Joey Dumont                    <joey.dumont@gmail.com>        #
# Created:      Feb. 22nd, 2018                                               #
# Description:  We streamline the process of submitting parameter sweeps on   #
#               SLURM systems. This Python script will read parameters from a #
#               text file, replace their values in a given template SLURM     #
#               submission script, create directory per simulation, and submit#
#               the simulations with sbatch.                                  #
# --------------------------------------------------------------------------- #

# --------------------------- Modules Importation --------------------------- #
import os
import re
import argparse
import subprocess
import shutil
import yaml

# ------------------------------ Configuration ------------------------------ #
# --------------------------- Function Definition --------------------------- #

def ConstructDirnameFromLoopIndex(config_file,loop_index):
	"""
	This returns the name of the created directory as a function of the loop
	index, where the loop_index is in [0,number_parameters-1].
	"""
	try:
		batchName = config_file['Batch Name']
	except KeyError:
		print("Your configuration file MUST specify a batch name.")
		raise

	# Folder names à la BQTools.
	dirname = batchName+"-{:05g}.SW".format(loop_index+1)

	return dirname

# ----------------------------- Argument Parsing ---------------------------- #
parser = argparse.ArgumentParser(usage='%(prog)s yaml_config_file')
parser.add_argument("config",
					type=str,
					help="YAML config file where the template files, the files \
							to be copied or linked are listed.")
parser.add_argument("-x",dest='CreateDirsOnly', action='store_true', default=False)
args = parser.parse_args()

# ----------------------------- Initialization ------------------------------ #

# -- Open the config file.
try:
	configFile = open(args.config, 'r')
except OSError:
	raise OSError("Your configuration file {} could not be opened. Make sure it exists.".format(args.config))

yamlFile   = yaml.load(configFile)

# -- Determine the number of parameters to sweep over.
##TODO: Multiple data files.
try:
	parameterFile = open(yamlFile['Data Files'][0], 'r')
except OSError:
	raise OSError("Your data file {} could not be opened. Make sure it exists.".format(yamlFile['DataFiles'][0]))

headerParameter = parameterFile.readline()

# Sanitize the line by removing everything before the first word.
headerParameter = re.sub(r"^([^\w]+)",
	                     "",
	                     headerParameter)

# Compute the number of parameters.
##TODO: Don't match final whitespace (why is it even there?)
split_header = re.split('\s+',headerParameter)[0:-1]
n_parameters = len(split_header)

# Number of lines of the file determines the number of directories created.
data_lines = parameterFile.read().splitlines()
size_sweep = len(data_lines)

# -- Create the directory structure.
for i in range(size_sweep):
	dirname = ConstructDirnameFromLoopIndex(yamlFile, i)
	if not os.path.exists(dirname):
		os.makedirs(dirname)

	symLinkName = dirname[0:dirname.rfind("-")+1]
	for j in range(n_parameters):
                symLinkName += split_header[j]+"{}".format(data_lines[i].split()[j])
	symLinkName += ".SW"

	if not os.path.exists(symLinkName):
		os.symlink(dirname,symLinkName)

# -- Copy the necessary files into the directory structure.
for i in range(size_sweep):
	dirname = ConstructDirnameFromLoopIndex(yamlFile, i)

	# Catch key errors and continue.
	try:
		for file in yamlFile['Copied Files']:
			shutil.copy(file, dirname)
	except KeyError:
		continue

# -- Link the necessary files into the directory structure.
for i in range(size_sweep):
	dirname = ConstructDirnameFromLoopIndex(yamlFile, i)
	os.chdir(dirname)
	try:
		for file in yamlFile['Linked Files']:
			if os.path.exists(file):
				os.remove(file)
			os.symlink("../"+file,file)
	except KeyError:
		continue
	os.chdir("../")

# -- String substitution in all template files.
try:
	collection = [yamlFile['Batch File'][0]]
except KeyError:
	print("Your config file MUST name a batch file.")
	raise

try:
	collection += [item for item in yamlFile['Template Files']]
except KeyError:
	pass

for template_file in collection:

	# Read the file
	with open(template_file, 'r') as template:
		template_lines = template.readlines()

	# Write the file with subsitutions in proper directory.
	for i in range(size_sweep):
		dirname = ConstructDirnameFromLoopIndex(yamlFile, i)

		# Open the file for writing.
		with open(dirname+"/"+template_file, 'w') as out_template:
			for template_line in template_lines:
				sub_template_line = template_line
				for j in range(n_parameters):
					sub_template_line = re.sub(r"~~"+split_header[j]+"~~",
						                       data_lines[i].split()[j],
					    	                   sub_template_line)
				out_template.write(sub_template_line)
			out_template.write("\n")

# -- Call srun in each directory.
if not args.CreateDirsOnly:
	for i in range(size_sweep):
		dirname = ConstructDirnameFromLoopIndex(yamlFile, i)
		os.chdir(dirname)

		# -- Create the files for stdout and stderr.
		std_basename = "{}-{:05g}".format(yamlFile['Batch Name'],i+1)
		f_stdout = open(std_basename+".out", 'w')
		f_stderr = open(std_basename+".err", 'w')
		try:
			proc = subprocess.check_call(["sbatch", "{}".format(yamlFile['Batch File'][0])],
			                         	 stdin=None,
			                         	 stdout=f_stdout,
			                         	 stderr=f_stderr)
		except FileNotFoundError:
			print("Your cluster does not seem to support SLURM. Start your jobs manually.")
			pass
		except subprocess.CalledProcessError:
			pass
		f_stdout.close()
		f_stderr.close()
		os.chdir("../")
