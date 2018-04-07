import numpy
import pandas as pd

print('done with imports')

def call_data():
	global datab
	global ub
	print('calling data')
	datab = pd.read_excel("slashdot_excel.xlsx", sheet_name=None)
	print('gathered data')
	print('calling nodes')
	ub = pd.read_excel("nodes.xlsx", sheet_name=None)
	print('gathered nodes')

call_data()
nodes = []
out_count = []
for index in ub:
	alpha_index = 0
	match_flag = 0
	skip_flag = 0
	count = 0
	break_flag = 0
	print('Done with ', index/len(ub)*100, ' %')
	while alpha_index < len(datab):
		if break_flag == 1:
			break_flag = 0
			break
		if datab['FromNodeId'][alpha_index] == index:
			if skip_flag == 1:
				alpha_index = alpha_index - 10000
				skip_flag = 0
			else:
				match_flag = 1
				count = count + 1
				alpha_index = alpha_index + 1

		else:
			if match_flag == 1:
				break_flag = 1
				match_flag =0
			if datab['FromNodeId'][index] < index:
				if(alpha_index + 10000) < len(datab):
					alpha_index = alpha_index +10000
					skip_flag = 1
			if datab['FromNodeId'][index] > index:
				if skip_flag == 1:
					alpha_index = alpha_index - 10000
					skip_flag = 0
	nodes.append(datab['FromNodeId'][index])
	out_count.append(count)

arr = numpy.asarray(nodes)
numpy.savetxt("nodes.csv", arr, delimiter=",")
arr1 = numpy.asarray(out_count)
numpy.savetxt("out_count.csv", arr1, delimiter=",")
