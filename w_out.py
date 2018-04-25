import numpy
import pandas as pd
import json
from pprint import pprint

print('done with imports')

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)

def call_data():
	global datab
	global ub
	print('collecting data')
	datab = pd.read_excel("slashdot_excel.xlsx", sheet_name=None)
	print('collected data')
	print('collecting nodes')
	ub = pd.read_excel("u_nodes.xlsx", sheet_name=None)
	print('collected nodes')

call_data()
nodes = []
out_count = []
positive_count = []
negative_count = []
iarray = []
lnd = int(len(datab))
lng = int(len(ub['Node']))
for i in range(0, lng):
	index = ub['Node'][i]
	#print('index', index)
	alpha_index = 0
	match_flag = 0
	skip_flag = 0
	loop_flag = 0
	count = 0
	count_p = 0
	count_n = 0
	break_flag = 0
	chk_flag = 0
	w_array = []
	if index < 10540:
		if index < 5102:
			if index > 197:
				alpha_index = 10000
			if index > 653:
				alpha_index = 20000
			if index > 893:
				alpha_index = 30000
			if index > 1064:
				alpha_index = 40000
			if index > 1270:
				alpha_index = 50000
			if index > 1507:
				alpha_index = 60000
			if index > 1838:
				alpha_index = 70000
			if index > 2173:
				alpha_index = 80000
			if index > 2571:
				alpha_index = 90000
			if index > 2907:
				alpha_index = 100000
		else:
			alpha_index = 150000
			if index > 7631:
				alpha_index  = 200000
	else:
		alpha_index = 250000
		if index > 14908:
			alpha_index = 300000
		if index > 19842:
			alpha_index = 350000
		if index > 26487:
			alpha_index = 400000
		if index > 36143:
			alpha_index = 450000
		if index > 54534:
			alpha_index = 500000
	
	while alpha_index < len(datab):
		if datab['FromNodeId'][alpha_index] == index:
			if skip_flag == 1:
				alpha_index = alpha_index - 500
				#print('case 1')
				skip_flag = 0
				chk_flag = 1
			else:
				match_flag = 1
				entry = (datab['ToNodeId'][alpha_index])
				w_array.append(entry)
