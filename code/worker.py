import threading
import socket
import sys
import json
import signal
import time
import copy

masterPort = 5001

workerPort = int(sys.argv[1])
worker_id = sys.argv[2]


"""
Setting up socket with which Worker receives from master the tasks
"""
fromMasterCommunicationSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
fromMasterCommunicationSocket.bind(('',workerPort))


"""
Thread lock set up to ensure no memory access issues causing race conditions
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
                if int(task['duration']) == 0:
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
            # Remove all tasks that have completed execution from the pool
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
    
    fromMasterCommunicationSocket.close()
    sys.exit(1)


    
sig_int = signal.getsignal(signal.SIGINT)
signal.signal(signal.SIGINT,close_sockets)