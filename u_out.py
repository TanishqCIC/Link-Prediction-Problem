import numpy
import pandas as pd

print('done with imports')

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
lng = int(len(ub['Node']))
for i in range(0, lng):
	index = ub['Node'][i]
	#print('index', index)
	alpha_index = 0
	match_flag = 0
	skip_flag = 0
	loop_flag = 0
	count = 0
	break_flag = 0
	chk_flag = 0
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
				count = count + 1
				#print('case 2')
				alpha_index = alpha_index + 1
				#print('match count', alpha_index)

		else:
			if match_flag == 1:
				break_flag = 1
				match_flag =0
				#print('match end here', alpha_index)
			if datab['FromNodeId'][alpha_index] < index:
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
							#print(datab['FromNodeId'][alpha_index-500] , '<', index)
						else:
							alpha_index = alpha_index + 1
							#print('case break point')
					else:
						skip_flag = 0
						alpha_index = alpha_index + 1
			if datab['FromNodeId'][alpha_index] > index:
				if skip_flag == 1:
					alpha_index = alpha_index - 500
					skip_flag = 0
					loop_flag = 1
					#print(datab['FromNodeId'][alpha_index+500] , '>', index)
				elif loop_flag == 1:
					break_flag = 1
					loop_flag = 0
					
					#print('case 4')
		if break_flag == 1:
			break_flag = 0
			alpha_index = len(datab)+1
			#print('while break')
	nodes.append(index)
	out_count.append(count)
	if i%1000 == 0:
		print('Done with ', i/lng*100, ' % with node ', index, ' count ', count)
	if i%10000 == 0:
		arr = numpy.asarray(nodes)
		numpy.savetxt("nodes.csv", arr, delimiter=",")
		arr1 = numpy.asarray(out_count)
		numpy.savetxt("out_count.csv", arr1, delimiter=",")		


arr = numpy.asarray(nodes)
numpy.savetxt("nodes.csv", arr, delimiter=",")
arr1 = numpy.asarray(out_count)
numpy.savetxt("out_count.csv", arr1, delimiter=",")
