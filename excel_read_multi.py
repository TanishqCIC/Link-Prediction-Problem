import threading
import numpy
import pandas as pd
import multithread as mt 
print('done with imports')

def call_data():
	global datab
	print('calling data')
	datab = pd.read_excel("slashdot_excel.xlsx", sheet_name=None)
	print('gathered data')
	

class myThread (threading.Thread):
	def __init__(self, threadID, name, counter):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.counter = counter
	def run(self):
	   	flag = self.threadID 
	   	"""print('calling data for thread ', flag)
	   		   		   		   	data = call_data()
	   		   		   		   	print('gathered data')"""
	   	if flag is 1:
	   		        data = datab[:50000]
	   		        thread_multi_1(data)
	   	if flag is 2:
	   		        data2 = datab[50000:100000]
	   		        thread_multi_2(data2)
	   	if flag is 3:
	   		data = datab[100000:150000]
	   		thread_multi_3(data)
	   	if flag is 4:
	   		data = datab[150000:200000]
	   		thread_multi_4(data)
	   	if flag is 5:
	   		data = datab[200000:250000]
	   		thread_multi_5(data)
	   	if flag is 6:
	   		data = datab[250000:300000]
	   		thread_multi_6(data)
	   	if flag is 7:
	   		data = datab[300000:350000]
	   		thread_multi_7(data)
	   	if flag is 8:
	   		data = datab[350000:400000]
	   		thread_multi_8(data)
	   	if flag is 9:
	   		data = datab[400000:450000]
	   		thread_multi_9(data)
	   	if flag is 10:
	   		data = datab[450000:500000]
	   		thread_multi_10(data)
	   	if flag is 11:
	   		data = datab[500000:]
	   		thread_multi_11(data)
		
      
      

	
def thread_multi_1(data):
	print('length of data ', len(data), ' thread 1')
	print(data['FromNodeId'][1])
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 1')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_1.csv", arr, delimiter=",")

def thread_multi_2(data):
	print('length of data ', len(data), ' thread 2')
	unique2 = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 2')
		if index == 0:
			unique2.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique2.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr2 = numpy.asarray(unique2)
	numpy.savetxt("multi_thread_2.csv", arr2, delimiter=",")

def thread_multi_3(data):
	print('length of data ', len(data), ' thread 3')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 3')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_3.csv", arr, delimiter=",")

def thread_multi_4(data):
	print('length of data ', len(data), ' thread 4')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 4')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_4.csv", arr, delimiter=",")

def thread_multi_5(data):
	print('length of data ', len(data), ' thread 5')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 5')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_5.csv", arr, delimiter=",")

def thread_multi_6(data):
	print('length of data ', len(data), ' thread 6')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 6')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_6.csv", arr, delimiter=",")

def thread_multi_7(data):
	print('length of data ', len(data), ' thread 7')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 7')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_7.csv", arr, delimiter=",")

def thread_multi_8(data):
	print('length of data ', len(data), ' thread 8')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 8')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_8.csv", arr, delimiter=",")

def thread_multi_9(data):
	print('length of data ', len(data), ' thread 9')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 9')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_9.csv", arr, delimiter=",")


def thread_multi_10(data):
	print('length of data ', len(data), ' thread 10')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 10')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_10.csv", arr, delimiter=",")

def thread_multi_11(data):
	print('length of data ', len(data), ' thread 11')
	unique = []
	alpha_index_flag = 0
	for index in range(0, len(data)):
		if index % 10000 == 0:
			print('Done ', index/len(data), ' %  of thread 11')
		if index == 0:
			unique.append(data['FromNodeId'][index])
		else:
			for alpha_index in unique:
				if data['FromNodeId'][index] == alpha_index:
					alpha_index_flag = 0
					break
				else:
					alpha_index_flag = 1
			if alpha_index_flag == 1:
				unique.append(data['FromNodeId'][index])
				alpha_index_flag = 0
	arr = numpy.asarray(unique)
	numpy.savetxt("multi_thread_11.csv", arr, delimiter=",")


call_data()

threadLock = threading.Lock()
threads = []



# Create new threads
thread1 = myThread(1, "Thread-1", 1)
thread2 = myThread(2, "Thread-2", 2)
thread3 = myThread(3, "Thread-3", 3)
thread4 = myThread(4, "Thread-4", 4)
thread5 = myThread(5, "Thread-5", 5)
thread6 = myThread(6, "Thread-6", 6)
thread7 = myThread(7, "Thread-7", 7)
thread8 = myThread(8, "Thread-8", 8)
thread9 = myThread(9, "Thread-9", 9)
thread10 = myThread(10, "Thread-10", 10)
thread11 = myThread(11, "Thread-11", 11)



# Start new Threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
thread7.start()
thread8.start()
thread9.start()
thread10.start()
thread11.start()

# Add threads to thread list
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)
threads.append(thread5)
threads.append(thread6)
threads.append(thread7)
threads.append(thread8)
threads.append(thread9)
threads.append(thread10)
threads.append(thread11)


# Wait for all threads to complete
for t in threads:
    t.join()
#print ("Exiting Main Thread")
