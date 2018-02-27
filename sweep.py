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
	batchName = config_file['Batch Name']
	extensionIndex = batchName.rfind(".")
	if extensionIndex < 0:
		extensionIndex = len(batchName)
	dirname = batchName[0:extensionIndex]+"-{:05g}.SW".format(loop_index+1)

	return dirname


# ----------------------------- Argument Parsing ---------------------------- #
parser = argparse.ArgumentParser(usage='$(prog)s yaml_config_file')
parser.add_argument("config",
					type=str,
					help="YAML config file where the template files, the files \
							to be copied or linked are listed.")
args = parser.parse_args()

# ----------------------------- Initialization ------------------------------ #

# -- Open the config file.
configFile = open(args.config, 'r')
yamlFile   = yaml.load(configFile)

# -- Determine the number of parameters to sweep over.
parameterFile = open(yamlFile['Data Files'][0], 'r')
headerParameter = parameterFile.readline()

# Sanitize the line by removing everything before the first word.
headerParameter = re.sub(r"^([^\w]+)",
	                     "",
	                     headerParameter)

# Compute the number of parameters.
##TODO: Multiple data files.
split_header = headerParameter.split()
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

	for file in yamlFile['Copied Files']:
		shutil.copy(file, dirname)


# -- Link the necessary files into the directory structure.
for i in range(size_sweep):
	dirname = ConstructDirnameFromLoopIndex(yamlFile, i)
	os.chdir(dirname)
	for file in yamlFile['Linked Files']:
		if os.path.exists(file):
			os.remove(file)
		os.symlink("../"+file,file)
	os.chdir("../")

# -- String substitution in all template files.
collection = [yamlFile['Batch File'][0]] + [item for item in yamlFile['Template Files']]

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
for i in range(size_sweep):
	dirname = ConstructDirnameFromLoopIndex(yamlFile, i)
	os.chdir(dirname)
	proc = subprocess.Popen(["sbatch", "{}".format(yamlFile['Batch File'][0])])
	os.chdir("../")
