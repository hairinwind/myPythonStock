import time
import random

from multiprocessing import Process, Queue, current_process, freeze_support

def runToAllDone(func, argIter, result_queue=None, NUMBER_OF_PROCESSES = 4):
    task_queue = Queue()
    allProcess = []
    for arg in argIter:
        task_queue.put((func, arg))

    # Start worker processes
    for i in range(NUMBER_OF_PROCESSES):
        p = Process(target=worker, args=(task_queue, result_queue))
        allProcess.append(p)
        p.start()
        task_queue.put('STOP')
    
    for p in allProcess:
        p.join()   
    
#
# Function run by worker processes
#

def worker(input, output):
    for func, args in iter(input.get, 'STOP'):
        result = calculate(func, args)
        if output != None:
            output.put(result)

#
# Function used to calculate result
#

def calculate(func, args):
    result = func(*args)
    return '%s says that %s%s = %s' % \
        (current_process().name, func.__name__, args, result)

#
# Functions for testing
#
def square(a):
    time.sleep(0.5*random.random())
    if a == 18:
        time.sleep(2) 
    print("{} : square {}".format(current_process().name, a))
    return a*a

def test1():
    runToAllDone(square, [(i,) for i in range(20)])
    print('all done...1')
    runToAllDone(square, [(i,) for i in range(100,105)])
    print('all done...2')

if __name__ == '__main__':
    freeze_support()
    test1()