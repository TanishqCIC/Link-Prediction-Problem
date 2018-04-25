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
	datab = pd.read_excel("slashdot_excel_to.xlsx", sheet_name=None)
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
	if index < 5112:
		if index < 1988:
			if index > 47:
				alpha_index = 10000
			if index > 83:
				alpha_index = 20000
			if index > 191:
				alpha_index = 30000
			if index > 452:
				alpha_index = 40000
			if index > 636:
				alpha_index = 50000
			if index > 706:
				alpha_index = 60000
			if index > 819:
				alpha_index = 70000
			if index > 937:
				alpha_index = 80000
			if index > 1052:
				alpha_index = 90000
			if index > 1146:
				alpha_index = 100000
		else:
			alpha_index = 150000
			if index > 3277:
				alpha_index  = 200000
	else:
		alpha_index = 250000
		if index > 7687:
			alpha_index = 300000
		if index > 11206:
			alpha_index = 350000
		if index > 16513:
			alpha_index = 400000
		if index > 25288:
			alpha_index = 450000
		if index > 40591:
			alpha_index = 500000
	
	while alpha_index < len(datab)-2:

		#print('case zero ', alpha_index)
		if datab['ToNodeId'][alpha_index] == index:
			if skip_flag == 1:
				alpha_index = alpha_index - 500
				#print('case 1')
				skip_flag = 0
				chk_flag = 1
			else:
				match_flag = 1
				entry = (datab['FromNodeId'][alpha_index])
				w_array.append(entry)
				#print('case 2')
				alpha_index = alpha_index + 1
				#print('match count', alpha_index)

		else:
			if match_flag == 1:
				break_flag = 1
				match_flag =0
				#print('match end here', alpha_index)
			if datab['ToNodeId'][alpha_index] < index:
				if loop_flag == 1:
					alpha_index = alpha_index + 1
					loop_flag = 1
					skip_flag = 0
					#print('case 3', alpha_index)
				else:
					if chk_flag != 1:
						if(alpha_index + 500) < len(datab):
							alpha_index = alpha_index +500
							skip_flag = 1
							#print(datab['ToNodeId'][alpha_index-500] , '<', index)
						else:
							alpha_index = alpha_index + 1
							#print('case break point')
					else:
						skip_flag = 0
						#print('skip point')
						if alpha_index + 1 <lnd:
							alpha_index = alpha_index + 1
			if datab['ToNodeId'][alpha_index] > index:
				if skip_flag == 1:
					alpha_index = alpha_index - 500
					skip_flag = 0
					loop_flag = 1
					#print(datab['ToNodeId'][alpha_index+500] , '>', index)
				elif loop_flag == 1:
					break_flag = 1
					loop_flag = 0		
					#print('case 4')
				else:
					#print('hifi ')
					alpha_index = alpha_index + 1
		if break_flag == 1:
			break_flag = 0
			alpha_index = len(datab)+1
			iarray.append({'array': w_array, 'node': index})
			#iarray = type(np.float64(0).item(iarray))
			#print('while break')
	if i%1000 == 0:
		print('Done with ', i/lng*100, ' % with node ', index, ' count ', count)
	if i%10000 == 0:
		with open('w_in_array.json', 'w') as fh:
			json.dump(iarray, fh, cls = MyEncoder)

with open('w_in_array.json', 'w') as fh:
	iarray = list(iarray)
	json.dump(iarray, fh, cls = MyEncoder)
