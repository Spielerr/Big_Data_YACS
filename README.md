# Big Data Project
## Yet Another Centralized Scheduler (YACS)

### Team Members

* Darshan D
* Karan Kumar G
* Manu M Bhat
* Mayur Peshve

**Team ID**: BD_1439_1452_1456_1883

## Summary

A project aimed at implementing a centralized scheduling framework that maintains and manages a master node and multiple worker nodes, all simulated as different processes on a single machine. The Master node receives job requests, which are scheduled on multiple slots across the available worker machines. The Master process consists of separate threads to listen to requests, to schedule map, and reduce tasks, and to listen to job completion information from workers. The Worker process listens to job allocation information and simulates execution. The job requests consist of map and reduce tasks with the dependency that the reduce tasks be executed only after the completion of the corresponding map tasks. The project involved the implementation of 3 different scheduling algorithms, namely Least loaded, Round robin, and Randomised Scheduling algorithms. This project was developed and implemented as part of the Big Data Course during our Undergraduate Study. 

## Implementation Details

The project involved the implementation of two Python programs - master.py and worker.py which perform the functions of the Master and a Worker machine respectively. These programs are independent of each other and can run on separate machines if required.

### master.py

This program essentially consists of 4 separate threads to carry out the following tasks:
 1. **JobListener**: Listens to job requests
 2. **JobScheduler**: Starts a job and schedules its map tasks on the workers
 3. **ReduceTaskScheduler**: Schedules the reduce tasks on the workers
 4. **WorkerManager**: Listens to job completion updates from the workers

It also contains the implementation of the 3 scheduling algorithms (Random, Round-Robin, and Least Loaded).

The JobListener thread performs the following tasks in order:
 * Listen to incoming job requests through specified port (in this case, port 5000).
 * Initially append only the map tasks into the Map tasks queue. 

The JobScheduler thread performs the following tasks in order:
 * Run the scheduler function to determine the workers to which the map tasks can be allocated and then send the current task information to the allotted Worker.
 * Update new allotment information by calling the Scheduler function again. Repeat until all the map tasks are allotted to Workers.
 * If there are no free slots available in any of the workers (as returned from the Scheduler function), make the Master wait for 1 second, before it again polls and tries to find if any slots become free.

The ReduceTaskScheduler thread performs the following tasks in order:
 * Run the scheduler function to determine the workers to which the reduce tasks can be allocated and then send the current task information to the allotted Worker.
 * Update new allotment information by calling the Scheduler function again. Repeat until all the reduce tasks are allotted to Workers.
 * If there are no free slots available in any of the workers (as returned from the Scheduler function), make the Master wait for 1 second, before it again polls and tries to find if any slots become free.

The WorkerManager thread performs the following tasks in order:
 * Listen to workers regarding updates about task completions. Update related information accordingly (increase free slot count, reduce the count of tasks to be allocated).
 * If all the map tasks of a job are completed, only then append the corresponding reduce tasks of the same job to the reduce tasks queue, which can then be scheduled thereby preserving the task dependency criteria.
 * If the final reduce task is finished, then that particular job is completed and appropriately logged.

The important events with corresponding timestamps logged are as follows:
 * Binding sockets to setup communication between master and job-requests-sender, master and worker
 * Scheduler type for analysis reference
 * When no slots in worker machines are free
 * Starting a new job
 * Sending and receiving task information to and from Workers 
 * Ending a job

### worker.py

This program simulates the working of the Worker machines. The same program is run multiple times simultaneously to simulate the simultaneous running of multiple Worker machines. This program essentially listens to the job allocation information from the master (on one thread, with a specified port number), and simulates the execution of these tasks by decrementing the task duration and sleeping for 1 second (on a second thread). Meanwhile, it continues to listen to the Master (on the other thread), while each of the slots continues executing their tasks.

Each of the workers logs events into the log file. They log the following information:
 * Starting a map/reduce task and recording the timestamp
 * Ending a map/reduce task and recording the timestamp

## Results and Analysis

The task and job execution times shed light on the type of scheduling algorithm used. The tests were run with 3 worker machines, with 3, 7, and 9 slots respectively, for 30 job requests.

The observations were as follows:
 * The Least Loaded scheduling algorithm has the least mean job completion time.
 * The job completion times for Random scheduling vary, highlighting the random nature of this type of scheduling.
 * The Round-Robin scheduling algorithm generally takes the longest time to finish job execution.

## Details about running the Software:

* The implementation files are placed inside the **code** folder of this repository
* The master can be executed as follows:
```python3 master.py config.json scheduling_algorithm```
* Specify the scheduling algorithm as LL/RR/RANDOM in place of "scheduling_algorithm"
* The workers can be executed as follows:
```
python3 worker.py 4000 1
python3 worker.py 4001 2
python3 worker.py 4002 3
```
* The analysis.csv with the information required to plot can be generated as follows:

```python3 analysis.py master_log_file.log worker_log_file.log ../analysis/analysis.csv```

* The bar graphs for Part 2 Result 1 can be plotted as follows:

```python3 analysis.py master_log_file.log worker_log_file.log ../analysis/analysis.csv 1```

* The heatmaps for Part 2 Result 2 can be plotted as follows:

```python3 analysis.py master_log_file.log worker_log_file.log ../analysis/analysis.csv 2```
