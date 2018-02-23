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

# ------------------------------ Configuration ------------------------------ #

# ----------------------------- Argument Parsing ---------------------------- #
parser = argparse.ArgumentParser()
parser.add_argument("--template",
					type=str,
					dest='template',
					help="Template file where we the parameters will be \
						 substituted")
parser.add_argument("--parameters",
					type=str,
					dest='parameters',
					help="Headered file that contains the parameters over \
					      which we will sweep.")

args = parser.parse_args()

# ----------------------------- Initialization ------------------------------ #

# -- Open the template file.
with open(args.template) as template:
	template_lines = template.readlines()

# -- Determine the number of parameters to sweep over (number of columns).
f = open(args.parameters)
header = f.readline()

# Sanitize the line.
header = re.sub(r"^([^\w]+)",
	            "",
	            header)

# Number of parameters.
split_header = header.split()
n_parameters = len(header.split())

# -- Create the directory structure.
lines = f.read().splitlines()
size_sweep = len(lines)

for i in range(size_sweep):
	extensionIndex = args.parameters.rfind(".")
	if extensionIndex < 0:
		extensionIndex = len(args.parameters)
	dirname = args.parameters[0:extensionIndex]+"-{:05g}.BQ".format(i+1)
	if not os.path.exists(dirname):
		os.makedirs(dirname)

	symlinkName = args.parameters[0:extensionIndex]+"-"
	for j in range(n_parameters):
		symlinkName += split_header[j]+"{}".format(lines[i].split()[j])
	symlinkName += ".BQ"

	if not os.path.exists(symlinkName):
		os.symlink(dirname,symlinkName)

	# -- String Substitution
	# -- For each line of the file, determine if there is parameter and replace
	# -- it with value in the parameters file, for each line of the parameters file.
	with open(dirname+"/"+args.template, 'w') as out_template:
		for template_line in template_lines:
			sub_template_line = template_line
			for j in range(n_parameters):
				sub_template_line = re.sub(r"~~"+split_header[j]+"~~",
					                         lines[i].split()[j],
					                         sub_template_line)
			out_template.write(sub_template_line)
		out_template.write("\n")

	# -- Call the batch job.
	os.chdir(dirname)
	proc = subprocess.Popen(["srun {}".format(args.template)],
             stdin=None, stdout=None, stderr=None, close_fds=True)
	os.chdir("../")
