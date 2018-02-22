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
