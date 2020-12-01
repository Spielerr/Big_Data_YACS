"""
Import the necessary libraries
"""
import matplotlib.pyplot as plt
import numpy as np
import re
import sys
from datetime import *
import seaborn as sns
import pandas as pd
import json
"""
Store all information required from log file into a dictionary
Scheduler stores the scheduler name(Random/ Round Robin/ Least Loaded)
"""
log_dict={}
Scheduler = ""

configFile = open("config.json","r")
configJson = json.load(configFile)
configFile.close()
"""
Regex to match and acquire necessary information to calculate statistical values and plot graphs
"""
def mymatch(pat, text):
	m = re.search(pat, text)
	if m :
		date_ = m.group(1)
		time = m.group(5)
		t = date_ + " " + time

		log_message = m.group(12)

		pat_1 = r'\s*Job\s*job_id\s*=\s*(\d*)'
		pat_2 = r'\s*(Map|Reduce).*?task_id\s*=\s*(.*?)\s*job_id\s*=\s*(\d*).*?(worker_id\s*=\s*(\d*))?$'
		m1 = re.search(pat_1, log_message)
		m2 = re.search(pat_2, log_message)
		job_id = -1

		if m1:
			job_id = m1.group(1)
			try:
				log_dict["Job " + job_id]["end_time"] = t
				d_t_obj1 = datetime.strptime(log_dict["Job " + job_id]["start_time"], '%Y-%m-%d %H:%M:%S.%f')
				d_t_obj2 = datetime.strptime(log_dict["Job " + job_id]["end_time"], '%Y-%m-%d %H:%M:%S.%f')
				d_t_obj = d_t_obj2 - d_t_obj1
				log_dict["Job " + job_id]["diff"] = round(d_t_obj.total_seconds()*1000)

			except Exception as e:
				if m.group(11) == "Arrival":
					log_dict["Job " + job_id] = {}
					log_dict["Job " + job_id]["job_id"] = job_id
					log_dict["Job " + job_id]["start_time_o"] = datetime.strptime(t, '%Y-%m-%d %H:%M:%S.%f')
					log_dict["Job " + job_id]["start_time"] = t
		elif m2:
			job_id = m2.group(3)
			try:
				log_dict["Job " + job_id][m2.group(2)]["end_time"] = t
				d_t_obj1 = datetime.strptime(log_dict["Job " + job_id][m2.group(2)]["start_time"], '%Y-%m-%d %H:%M:%S.%f')
				d_t_obj2 = datetime.strptime(log_dict["Job " + job_id][m2.group(2)]["end_time"], '%Y-%m-%d %H:%M:%S.%f')
				d_t_obj = d_t_obj2 - d_t_obj1
				log_dict["Job " + job_id][m2.group(2)]["diff"] = round(d_t_obj.total_seconds()*1000)

			except Exception as e:
				if m2.group(1)=="Map":
					try:
						log_dict["Job " + job_id]["No_of_M"] += 1
					except:
						log_dict["Job " + job_id]["No_of_M"] = 1
				else:
					try:
						log_dict["Job " + job_id]["No_of_R"] += 1
					except:
						log_dict["Job " + job_id]["No_of_R"] = 1
				log_dict["Job " + job_id][m2.group(2)] = {}
				log_dict["Job " + job_id][m2.group(2)]["start_time_o"] = datetime.strptime(t, '%Y-%m-%d %H:%M:%S.%f')
				log_dict["Job " + job_id][m2.group(2)]["start_time"] = t
				log_dict["Job " + job_id][m2.group(2)]["worker_id"] = m2.group(5)

"""
Regex to match the scheduler type to carry out analysis
"""
def mymatch2(pat, line):
	m = re.search(pat, line)
	global Scheduler
	if m:
		Scheduler = m.group(1)


"""
Fetch scheduler name for carrying out analysis on each of the sceduling algorithms
Write mean and median job and task completion times into a separate file and read it later for graphing purposes
"""
file_name1 = sys.argv[1]
file_name2 = sys.argv[2]

worker_id_list = []
workers = configJson["workers"]
for worker in workers:
	worker_id_list.append(worker["worker_id"])



file1 = open(file_name1, 'r')


pat = r"((\d{4})-(\d{2})-(\d{2}))\s*((\d{2}):(\d{2}):(\d{2}).(\d{3}))\s*(.*?)\s*(Arrival|Starting|Ending)(.*)"
pat2 = r".*?Scheduler\s*=\s*(.*?)$"
while 1:

	line = file1.readline()
	mymatch(pat, line)
	mymatch2(pat2, line)
	if not line:
		break
file1.close()

for i in worker_id_list:
	file_name = file_name2.split("_", 1)
	file_name = ("_" + str(i) +"_").join(file_name)
	file2 = open(file_name, 'r')
	while 1:

		line = file2.readline()
		mymatch(pat, line)
		mymatch2(pat2, line)
		if not line:
			break

	file2.close()

worker_t_c_t = []
job_t_c_t = []
machines = {}
for job_id in log_dict.keys():
	job_t_c_t.append(log_dict[job_id]["diff"])
	no_of_m = log_dict[job_id]["No_of_M"]
	no_of_r = log_dict[job_id]["No_of_R"]
	for i in range(no_of_m):
		try:
			machines[log_dict[job_id][log_dict[job_id]["job_id"] + "_M" + str(i)]["worker_id"]].append((log_dict[job_id][log_dict[job_id]["job_id"] + "_M" + str(i)]["start_time_o"] - log_dict["Job 0"]["start_time_o"]).total_seconds()*1000)
		except:
			machines[log_dict[job_id][log_dict[job_id]["job_id"] + "_M" + str(i)]["worker_id"]] = [(log_dict[job_id][log_dict[job_id]["job_id"] + "_M" + str(i)]["start_time_o"] - log_dict["Job 0"]["start_time_o"]).total_seconds()*1000]
		
		worker_t_c_t.append(log_dict[job_id][log_dict[job_id]["job_id"] + "_M" + str(i)]["diff"])
	for i in range(no_of_r):
		try:
			machines[log_dict[job_id][log_dict[job_id]["job_id"] + "_R" + str(i)]["worker_id"]].append((log_dict[job_id][log_dict[job_id]["job_id"] + "_R" + str(i)]["start_time_o"] - log_dict["Job 0"]["start_time_o"]).total_seconds()*1000)
		except:
			machines[log_dict[job_id][log_dict[job_id]["job_id"] + "_R" + str(i)]["worker_id"]] = [(log_dict[job_id][log_dict[job_id]["job_id"] + "_R" + str(i)]["start_time_o"] - log_dict["Job 0"]["start_time_o"]).total_seconds()*1000]		
		worker_t_c_t.append(log_dict[job_id][log_dict[job_id]["job_id"] + "_R" + str(i)]["diff"])

"""
Write job and task mean and median values to an external file
Since we have to run the entire process individually for all the three scheduling algorithms
"""


"""
Provide argument as 1 to generate the bar graphs for Part 2 Result 1
Reads from csv file consisting of the job and task mean and median completion times
"""
try:
	if sys.argv[4] == '1':
		"""
		Read the statistic values from the csv file into a 2d-array (statistic)
		"""
		"""
		Print mean and median task and job completion times for each scheduling algorithm 
		"""
		print("Task completion time mean: ", Scheduler, np.mean(worker_t_c_t))
		print("Task completion time median: ", Scheduler, np.median(worker_t_c_t))
		print("Job completion time mean: ", Scheduler, np.mean(job_t_c_t))
		print("Job completion time median: ", Scheduler, np.median(job_t_c_t))

		statistic = []
		algorithm = []
		file1 = open(sys.argv[3], "r")
		while 1:
			line = file1.readline()
			if not line:
				break
			l = line.split(",")
			algorithm.append(l[0])
			statistic.append(list(map(float, l[1::])))
		file1.close()

		"""
		Generate a grouped bar graph to show analysis
		"""
		font = {'size' : 25}
		plt.rc('font', **font)
		X = np.arange(4)
		plt.style.use('ggplot')
		fig, ax = plt.subplots(figsize=(5, 5))
		ax.set(xlabel = "Statistic", ylabel = "Time", title = "Statistically Analysis of Scheduling Algorithms")
		plt.bar(X + 0.00, statistic[0], color = '#0033cc', width = 0.2)
		plt.bar(X + 0.25, statistic[1], color = '#009900', width = 0.2)
		plt.bar(X + 0.50, statistic[2], color = '#800000', width = 0.2)
		plt.xticks(X + 0.25, ["Task Mean Completion Time", "Task Median Completion Time", "Job Mean Completion Time", "Job Median Completion Time"])
		plt.legend(algorithm, loc = 'best')
		plt.show()
		plt.savefig('../analysis/bar.png')
except:
	None


"""
Provide command line argument as 2 to calculate the mean and median task and job completion times
Generate line graphs and heatmaps to show analysis
"""
try:
	if sys.argv[4] == '2':
		"""
		To plot a simple line graph showing the cumulative number of tasks assigned
		for each worker machine
		Helpful for differentiating between the types of scheduling algorithms
		"""
		fig, ax = plt.subplots(figsize=(5, 5))
		# # Add x-axis and y-axis
		for k, v in machines.items():
			y = [i for i in range(1, len(v)+1)]
			# print(v, y)
			ax.plot(sorted(v), y, label = "Machine " + k)

		ax.set(xlabel = "Time", ylabel = "Number of Tasks", title = "Number of Tasks allocated per machine with time")
		plt.legend(loc = 'best')
		plt.show()
		plt.savefig('../analysis/' + Scheduler + '_line.png')

		"""
		Create dataframe consisting of worker ids, times of task arrivals and number of tasks
		For usage in displaying the heatmaps
		"""
		df = pd.DataFrame([], columns = ['Machine','Time of Task Arrival', 'Cumulative Number of Tasks'])
		for k, v in machines.items():
			a = 1
			for i in sorted(v):
				df.loc[len(df.index)] = [k, round(i/1000), a]
				a += 1

		"""
		Generate a heatmap showing the increasing number of tasks scheduled on each machine
		Considers the total number of tasks scheduled up to a certain point in time
		"""
		df1 = df.drop_duplicates(subset = ["Machine", "Time of Task Arrival"], keep = "last")
		df1 = df1.pivot("Machine", "Time of Task Arrival", "Cumulative Number of Tasks")
		df1 = df1.ffill(axis = 1)
		df1.fillna(0, inplace=True)
		sns.heatmap(df1, cmap = "rocket_r", cbar_kws = {"label" : "Number of Tasks Assigned", "orientation" : "horizontal"} )
		plt.title("Cumulative HeatMap Distributions for Number of Tasks Scheduled on each Machine")
		plt.xticks(rotation = 'horizontal')
		plt.show()
		plt.savefig('../analysis/' + Scheduler + '_heatmap1.png')

		"""
		Generate a heatmap showing the number of tasks scheduled on each machine for a specific intervals of time
		This here shows the number of tasks scheduled every 5 seconds on each of the worker machines
		"""
		df2 = df1.iloc[:, ::5]
		temp_df = df2[0]
		df2 = df2.diff(axis = 1)
		for i in range(len(temp_df)):
			df2[0][i] = temp_df[i]
		sns.heatmap(df2, cmap = "rocket_r", cbar_kws = {"label" : "Number of Tasks Assigned", "orientation" : "horizontal"} )
		plt.title("Discrete HeatMap Distributions for Number of Tasks Scheduled on each Machine")
		plt.xticks(rotation = 'horizontal')
		plt.show()
		plt.savefig('../analysis/' + Scheduler + '_heatmap2.png')
except:
	file1 = open(sys.argv[3], "a")
	file1.write("{},{},{},{},{}\n".format(Scheduler, np.mean(worker_t_c_t), np.median(worker_t_c_t), np.mean(job_t_c_t), np.median(job_t_c_t)))
	file1.close()
