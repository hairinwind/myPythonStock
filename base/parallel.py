from multiprocessing import Process, Queue, current_process, freeze_support
import random
import time

import datetime as dt


def runToAllDone(func, argIter, result_queue=None, NUMBER_OF_PROCESSES=4):
    start = dt.datetime.utcnow();
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
    
    end = dt.datetime.utcnow();
    delta = end - start
    print('use time:', delta.total_seconds())
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
    try:
        result = func(*args)
        return '%s says that %s = %s' % (current_process().name, args, result)
    except Exception as e:
            print("error happened when running multiple threads methods...")
            print(str(e))

#
# Functions for testing
#
def square(a):
    time.sleep(0.5 * random.random())
    if a == 18:
        time.sleep(2) 
    print("{} : square {}".format(current_process().name, a))
    return a * a

def test1():
    runToAllDone(square, [(i,) for i in range(20)])
    print('all done...1')
    runToAllDone(square, [(i,) for i in range(100, 105)])
    print('all done...2')

if __name__ == '__main__':
    freeze_support()
    test1()
