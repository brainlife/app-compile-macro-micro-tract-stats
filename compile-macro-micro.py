#!/usr/bin/env python3

import json
import subprocess
import pandas as pd
import numpy as np
import os, sys, argparse
import glob

def collectTrackMacroData(dataPath,subjectID):

    # set up variables
    data_columns = ['subjectID','nodeID','structureID','count','length','volume']

    # set up empty data frame
    macro_data = pd.DataFrame([])

    # load macro data
    macro_data_unclean = pd.read_csv(dataPath)
    macro_data_unclean['subjectID'] = [ subjectID for f in range(len(macro_data_unclean['TractName'])) ]
    macro_data_unclean['nodeID'] = [ 1 for f in range(len(macro_data_unclean['TractName'])) ]

    # append to output data structure
    macro_data = macro_data.append(macro_data_unclean,ignore_index=True)

    macro_data = macro_data[['subjectID','nodeID','TractName','StreamlineCount','avgerageStreamlineLength','volume']]
    macro_data.columns = data_columns
	
    return macro_data

def combineTrackMacroMicro(dataPath,macro_data,micro_data):

	# subject ID from profiles and macro data might not be saved as same datatype, making the merge not work properly. check and fix
	if list(micro_data['subjectID'].unique()) != list(macro_data['subjectID'].unique()):
		micro_data['subjectID'] = [ macro_data['subjectID'].unique()[0] for f in micro_data['subjectID'] ]

	# make sure structureID's are the same
	#if list(np.sort(macro_data['structureID'][macro_data['structureID'] != 'wbfg'].unique().tolist())) != list(np.sort(micro_data['structureID'].unique().tolist())):
		#macro_data['structureID'][macro_data['structureID'] != 'wbfg'] = [ f for f in list(micro_data['structureID'].unique()) ]
		#need to think of better sort function. for now don't worry about. should be fine in most cases
	
	# need to update for issues with quickbundles seg classifcation name (temp)
	if [ f.replace('_','') for f in macro_data['structureID'].unique() if f != 'wbfg' ] == list(np.sort(micro_data['structureID'].unique().tolist())):
		macro_data['structureID'] = [ f.replace('_','') for f in macro_data['structureID'].unique() ]
	
	# merge data frames
	data = pd.merge(micro_data,macro_data.drop(columns='nodeID'),on=['subjectID','structureID'])

	# output data structure for records and any further analyses
	if not os.path.exists(dataPath):
	    os.mkdir(dataPath)

	data.to_csv(dataPath+'/output_FiberStats.csv',index=False)

def main():

	print("setting up input parameters")
	#### load config ####
	with open('config.json','r') as config_f:
		config = json.load(config_f)

	#### parse inputs ####
	profiles_path = config['profiles']
	subjectID = config['_inputs'][0]['meta']['subject']
	macro_path = config['macro']

	#### set up other inputs ####
	# set outdir
	outdir = 'tractmeasures'
	
	# generate output directory if not already there
	if os.path.isdir(outdir):
		print("directory exits")
	else:
		print("making output directory")
		os.mkdir(outdir)

	#### run command to generate csv structures ####
	print("cleaning up macro data")
	macro_data = collectTrackMacroData(macro_path,subjectID)

	print("loading profiles data")
	profiles_data = pd.read_csv(profiles_path)

	print("generating csvs")
	combineTrackMacroMicro(outdir,macro_data[macro_data['structureID'] != 'wbfg'],profiles_data)

if __name__ == '__main__':
	main()
