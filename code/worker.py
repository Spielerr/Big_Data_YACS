import threading
import socket
import sys
import json
import signal
import time
import copy
import logging

masterPort = 5001

workerPort = int(sys.argv[1])
worker_id = sys.argv[2]

logging.basicConfig(filename="worker_" + worker_id + "_log_file.log", filemode= "w" ,format="%(name)s - %(asctime)s.%(msecs)03d %(levelname)s : %(message)s", datefmt='%Y-%m-%d %I:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

"""
Setting up socket with which Worker receives from master the tasks
"""
fromMasterCommunicationSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
fromMasterCommunicationSocket.bind(('',workerPort))


"""
Thread lock set up to restrict memory access protecting from race conditions
"""
threadLock = threading.Lock()


"""
Task Queue maintains the queue of tasks as arrived from master
"""
taskQueue = []

"""
Listens to master for task communication
Runs on a separate thread
"""
class Listener(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        while True:
            fromMasterCommunicationSocket.listen(1)
            masterSocket, _ = fromMasterCommunicationSocket.accept()
            taskData = masterSocket.recv(2048)
            taskData = json.loads(taskData)
            
            threadLock.acquire()
            if taskData["type"] == 'M':
                logger.info("Starting Map Task task_id = {} job_id = {} on Worker worker_id = {}".format(taskData['task_id'], taskData['job_id'], worker_id))
            else:
                logger.info("Starting Reduce Task task_id = {} job_id = {} on Worker worker_id = {}".format(taskData['task_id'], taskData['job_id'], worker_id))
            
            if int(taskData['duration']) <= 0:
                if taskData["type"] == 'M':
                    logger.info("Ending Map Task task_id = {} job_id = {} on Worker worker_id = {}".format(taskData['task_id'], taskData['job_id'], worker_id))
                else:
                    logger.info("Ending Reduce Task task_id = {} job_id = {} on Worker worker_id = {}".format(taskData['task_id'], taskData['job_id'], worker_id))

                taskData['worker_id'] = worker_id
                taskData = json.dumps(taskData)
                # Communicate with master about task completion
                toMasterCommunicationSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                toMasterCommunicationSocket.connect(("localhost",masterPort))
                toMasterCommunicationSocket.sendall(taskData.encode())
                toMasterCommunicationSocket.close()
            else:
                taskQueue.append(taskData)
            threadLock.release()
            

"""
Simulates execution of task by decrementing the duration
Runs on a separate thread to ensure master to worker communication channel is always available for listening to tasks
"""
class Worker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    
    def run(self):
        global taskQueue
        while True:
            """
            Temp queue to maintain the queue before acquiring threadlock to ensure 
            tasks getting decremented have spent atleast 1 second in execution cycle
            """
            temp_tq = copy.deepcopy(taskQueue) 
            time.sleep(1)
            threadLock.acquire()
            
            # Execution simulation
            for task in temp_tq:
                task['duration'] = task['duration'] - 1
                # Check if task execution completed
                if int(task['duration']) <= 0:
                    if task["type"] == 'M':
                        logger.info("Ending Map Task task_id = {} job_id = {} on Worker worker_id = {}".format(task['task_id'], task['job_id'], worker_id))
                    else:
                        logger.info("Ending Reduce Task task_id = {} job_id = {} on Worker worker_id = {}".format(task['task_id'], task['job_id'], worker_id))

                    task['worker_id'] = worker_id
                    task = json.dumps(task)
                    # Communicate with master about task completion
                    toMasterCommunicationSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    toMasterCommunicationSocket.connect(("localhost",masterPort))
                    toMasterCommunicationSocket.sendall(task.encode())
                    toMasterCommunicationSocket.close()

            # Add all tasks that were added to task queue during sleep of 1 second into task pool
            if len(taskQueue) > len(temp_tq):
                temp_tq.extend(taskQueue[len(temp_tq)::])
            # Remove all tasks completed execution from the pool
            taskQueue = [t for t in temp_tq if t['duration'] != 0]
            
            threadLock.release()

ListeningThread = Listener()
WorkThread = Worker()

ListeningThread.start()
WorkThread.start()

"""
Captures SIGINT to execute following function (Close_socket)
Closes sockets when Worker is terminated
"""
def close_sockets(signum,frame):
    signal.signal(signal.SIGINT, sig_int)
    print("Socket Closed")
    logger.debug("Closing Socket for Master Communication")
    fromMasterCommunicationSocket.close()
    logger.debug("Ending Worker")
    sys.exit(1)
   
sig_int = signal.getsignal(signal.SIGINT)
signal.signal(signal.SIGINT,close_sockets)