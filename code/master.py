import threading
import socket
import sys
import json
import signal
import logging
import random
import time

"""
Basic configuration set up of logger to appropriate logging scheme
"""
logging.basicConfig(filename="master_log_file.log", filemode= "w" ,format="%(name)s - %(asctime)s.%(msecs)03d %(levelname)s : %(message)s", datefmt='%Y-%m-%d %I:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
jobListenPort = 5000

"""
Sets up the port for listening to requests
"""
logger.debug("Binding Socket to listen to to port {} for jobs".format(jobListenPort))
jobListenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jobListenSocket.bind(('',jobListenPort))

"""
Sets up the port for listening to workers regarding information about task completion
"""
workerPort = 5001
logger.debug("Binding socket to listen to Workers")
workerSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerSocket.bind(("localhost",workerPort))

"""
Initialize the thread lock variable
"""
threadLock = threading.Lock()

"""
Take the config file and the scheduling algorithm to use as command line arguments
"""
configFileName = sys.argv[1]
schedulingAlgorithm = sys.argv[2]

"""
Read the config json file
"""
configFile = open(configFileName,"r")
configJson = json.load(configFile)
configFile.close()

"""
Initialize a job queue as a dictionary and a task queue as a list (both as global variables)
"""
jobQueue = {}
taskQueue = []

"""
Initalising workers from json to Worker Objects
"""
def initWorkers(config):
    workerList = []
    workers = config["workers"]
    for worker in workers:
        W = Worker(worker["port"],worker["worker_id"],worker["slots"])
        workerList.append(W)
    return workerList

"""
Schedulers To schedule tasks
"""
class Scheduler():
    def __init__(self, schedulerType, workers):
        self.roundRobinIndex = 0
        self.schedulerType = schedulerType
        self.workers = workers
        self.numOfWorkers = len(self.workers)

    """
    Wrapper function allowing for call without regard to specified scheduler type
    """
    def scheduler(self):
        if self.schedulerType == "RR":
            return self.RoundRobinScheduler()
        if self.schedulerType == "RANDOM":
            return self.RandomScheduler()
        if self.schedulerType == "LL":
            return self.LeastLoadedScheduler()

    """
    All the scheduling algorithms return worker object to assign task to
    """
    def RoundRobinScheduler(self):
        logger.debug("Round Robin Scheduling Task")
        initRRIndex = self.roundRobinIndex
        while self.workers[self.roundRobinIndex].slotsFree == 0:
            self.roundRobinIndex = (self.roundRobinIndex + 1) % self.numOfWorkers
            # Upon no free slots after one round of checking, None is returned
            if initRRIndex == self.roundRobinIndex:
                logger.debug("All Workers' slots full")
                return None
        # Updating slots of assigned worker
        self.workers[self.roundRobinIndex].slotsFree -= 1
        tempIndex = self.roundRobinIndex
        
        # Updates round robin index to next worker in queue
        self.roundRobinIndex = (self.roundRobinIndex + 1) % self.numOfWorkers
        return self.workers[tempIndex]


    def RandomScheduler(self):
        logger.debug("Random Scheduling Task")
        randomIndex = random.SystemRandom().randint(0, self.numOfWorkers - 1)
        count = 0
        while self.workers[randomIndex].slotsFree == 0:
            # After checking for free slots 'c' * number of available workers, if no free slots found
            # Then return None
            # count maintains the count of how many random worker ids generated
            c = 1.5 # Can be set up as seen fit, we have choosen 1.5 after some experimentation
            if count >= (self.numOfWorkers * c) :
                logger.debug("All Workers' slots full")
                return None
            count += 1
            randomIndex = random.SystemRandom().randint(0, self.numOfWorkers - 1)
        
        self.workers[randomIndex].slotsFree -= 1
        return self.workers[randomIndex]

    def LeastLoadedScheduler(self):
        worker_index = 0
        # Loop through all available workers and find least loaded worker
        for indx in range(1,self.numOfWorkers):
            if self.workers[worker_index].slotsFree < self.workers[indx].slotsFree:
                worker_index = indx
        # If no worker had free slot, return None
        if self.workers[worker_index].slotsFree == 0:
            logger.debug("All Workers' slots full")
            return None


        self.workers[worker_index].slotsFree -= 1
        return self.workers[worker_index]


"""
Maintains information about Jobs received
"""
class Job():
    def __init__(self,jobData):
        self.jobID = jobData['job_id']
        map_tasks = jobData['map_tasks']
        self.map_tasks = []
        for map_task in map_tasks:
            map_task['job_id'] = self.jobID
            map_task['type'] = "M"
            self.map_tasks.append(map_task)
        self.numOfMapTasks = len(self.map_tasks)
        reduce_tasks = jobData['reduce_tasks']
        self.reduce_tasks = []
        for reduce_task in reduce_tasks:
            reduce_task['job_id'] = self.jobID
            reduce_task['type'] = "R"
            self.reduce_tasks.append(reduce_task)
        self.numOfRedTasks = len(self.reduce_tasks)
        # Boolean to track if Job execution has been started
        self.jobStarted = False


"""
Maintains information about Worker
"""
class Worker():
    def __init__(self,portNo,workerID,slots):
        self.portNo = portNo
        self.workerID = workerID
        self.slots = slots
        self.slotsFree = slots


"""
Listens to job requests from specified port
Schedules tasks as seen fit according to the scheduler
Runs on separte thread to allow Worker Manager(defined later on) to communicate effectively with workers
"""
class JobScheduler(threading.Thread):
    def __init__(self,threadID,workers,scheduler):
        threading.Thread.__init__(self)
        self.name = "Job Listener"
        self.threadID = threadID
        self.workers = workers
        self.numOfWorkers = len(self.workers)
        self.scheduler = scheduler


    def __str__(self):
        return self.name +  str(self.threadID)


    def run(self):
        while True:
            # Listening for job requests
            jobListenSocket.listen(1)
            clientSocket, _ = jobListenSocket.accept()
            jobData = clientSocket.recv(2048)
            jobData = json.loads(jobData)

            job = Job(jobData)
            logger.info("Arrival Job job_id = {}".format(job.jobID))
            threadLock.acquire()

            jobQueue[job.jobID] = job
            # Adding map tasks to taskQueue
            for map_task in job.map_tasks:
                taskQueue.append(map_task)
            threadLock.release()

            threadLock.acquire()

            # Scheduling as many tasks as possible for execution
            # And communicating them to their respective workers as scheduled by scheduler

            worker = self.scheduler.scheduler()
            threadLock.release()
            while worker:
                # Task Queue empty, means stop scheduling task
                if not taskQueue:
                    worker.slotsFree += 1
                    break
                threadLock.acquire()
                task = taskQueue.pop(0)
                threadLock.release()
                if not jobQueue[task['job_id']].jobStarted:
                    logger.info("Starting Job job_id = {}".format(task['job_id']))
                    jobQueue[task['job_id']].jobStarted = True

                #Communication to workers
                toWorkerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                toWorkerSocket.connect(('',worker.portNo))
                task = json.dumps(task)

                toWorkerSocket.sendall(task.encode())
                toWorkerSocket.close()
                task = json.loads(task)
                if task['type'] == "M":
                    logger.info("Sending Map Task task_id = {} job_id = {} on Worker worker_id = {}".format(task['task_id'],task['job_id'],worker.workerID))
                else:
                    logger.info("Sending Reduce Task task_id = {} job_id = {} on Worker worker_id = {}".format(task['task_id'],task['job_id'],worker.workerID))
                threadLock.acquire()
                worker = self.scheduler.scheduler()
                threadLock.release()
			
            time.sleep(1)

"""
WorkerManager listens to workers and the info provided about task comppletion
Upon a job completing all it's map tasks (i.e dependencies for reduce tasks),
It adds the reduce task to the taskQueue as well, for execution
"""

class WorkerManager(threading.Thread):
    def __init__(self,threadID,workerList):
        threading.Thread.__init__(self)
        self.name = "Worker Manager"
        self.threadID = threadID
        self.workers = {str(worker.workerID):worker for worker in workerList}

    def run(self):
        while True:
            #Scheduling of all possible tasks
            threadLock.acquire()
            worker = scheduler.scheduler()
            threadLock.release()

            while worker:
                if not taskQueue:
                    worker.slotsFree += 1
                    break
                threadLock.acquire()
                task = taskQueue.pop(0)
                threadLock.release()
                if not jobQueue[task['job_id']].jobStarted:
                    logger.info("Starting Job job_id = {}".format(task['job_id']))
                    jobQueue[task['job_id']].jobStarted = True
                toWorkerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                toWorkerSocket.connect(('',worker.portNo))
                task = json.dumps(task)

                toWorkerSocket.sendall(task.encode())
                toWorkerSocket.close()
                task = json.loads(task)
                if task['type'] == "M":
                    logger.info("Sending Map Task task_id = {} job_id = {} on Worker worker_id = {}".format(task['task_id'],task['job_id'],worker.workerID))
                else:
                    logger.info("Sending Reduce Task task_id = {} job_id = {} on Worker worker_id = {}".format(task['task_id'],task['job_id'],worker.workerID))
                threadLock.acquire()
                worker = scheduler.scheduler()
                threadLock.release()
            time.sleep(1)

            # Listening to workers about task completion
            workerSocket.listen(len(self.workers) * 100)
            fromWorkerSocket, _ = workerSocket.accept()
            workerData = fromWorkerSocket.recv(2048)
            workerData = json.loads(workerData)

            threadLock.acquire()
            job = jobQueue[workerData['job_id']]
            self.workers[workerData['worker_id']].slotsFree += 1
            threadLock.release()
            # Updating job completion information
            if workerData['type'] == "M":
                logger.info("Received Map task task_id = {} job_id = {}".format(workerData['task_id'],workerData['job_id']))
                job.numOfMapTasks -= 1
                #If all ma tasks completed, adding reduce tasks to task pool for scheduling
                if job.numOfMapTasks == 0:
                    threadLock.acquire()
                    for reduce_task in job.reduce_tasks:
                        reduce_task['type'] = "R"
                        taskQueue.append(reduce_task)
                    threadLock.release()
            else:
                logger.info("Received Reduce task task_id = {} job_id = {}".format(workerData['task_id'],workerData['job_id']))
                job.numOfRedTasks -= 1
                #If all reduce tasks completed, means job completed
                #Updating the job completion information
                if job.numOfRedTasks == 0:
                    logger.info("Ending Job job_id = {}".format(workerData['job_id']))
                    threadLock.acquire()
                    del jobQueue[workerData['job_id']]
                    threadLock.release()


workers = initWorkers(configJson)

scheduler = Scheduler(schedulingAlgorithm,workers)
logger.info("Using Scheduler = {}".format(schedulingAlgorithm))


jobThread = JobScheduler(0,workers,scheduler)
workManagerThread = WorkerManager(1,workers)

jobThread.start()
workManagerThread.start()

"""
Captures SIGINT to execute following function (Close_socket)
Closes sockets when Worker is terminated
"""
def close_sockets(signum,frame):
    signal.signal(signal.SIGINT, sig_int)
    logger.debug("Sockets Closed")
    jobListenSocket.close()
    workerSocket.close()
    logger.info("Ending Master")
    sys.exit(1)


sig_int = signal.getsignal(signal.SIGINT)
signal.signal(signal.SIGINT,close_sockets)