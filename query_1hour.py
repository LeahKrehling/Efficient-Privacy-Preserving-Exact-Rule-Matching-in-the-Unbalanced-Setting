import hashlib
import math
import traceback
import sys
import time
from multiprocessing import Process, JoinableQueue, Queue, cpu_count, Event
from gmpy2 import mpz, powmod, is_prime, bit_set
from random import randint, getrandbits
from secrets import token_urlsafe
from queue import Empty, Full

class Blake2b(object):
    def __init__(self, output_bit_length=64, salt=None, optimized64=False):

        self.output_bit_length = output_bit_length
        try: 
            if self.output_bit_length > 512:
                raise ValueError("Output length exceeds maximum Blake2b digest size")
            if self.output_bit_length < 3:
                raise ValueError("Output length specifies invalid codomain")
        except ValueError:
            traceback.print_exc()
            sys.exit()

        digest_size = math.ceil(output_bit_length / 8)

        if salt is None:
            self.hash = hashlib.blake2b(digest_size=digest_size)
        else:
            self.hash = hashlib.blake2b(digest_size=digest_size, salt=salt)

        self.output_modulus = mpz(2**(self.output_bit_length - 1))

    def PrimeHash(self, preimage): 
        h = self.hash.copy() 
        h.update(preimage)  
        current_digest = h.digest()

        while True:
            U = int.from_bytes(current_digest, sys.byteorder) % self.output_modulus 
            candidate = bit_set(self.output_modulus + U, 0)

            if is_prime(candidate):
                return candidate

            h.update(b'0') 
            current_digest = h.digest()

    def KeyGen(self, preimage, size):
        h = self.hash.copy()
        h.update(preimage)
        digest = h.hexdigest()
        return int(digest, 16) % size


def task(bins, task_queue, stop_flag, ident,  f):
    jobs_done = 0
    while not stop_flag.is_set():
        try:
            next_task = task_queue.get(timeout=1)
        except Empty:
            continue
        
        expo, in_bin = next_task
        N, A = bins[in_bin]
        answer = powmod(A, expo, N)
        bins[in_bin][1] = answer

        task_queue.task_done()
        jobs_done += 1

    print(f'Consumer: {ident} completed: {jobs_done} jobs', flush=True, file=f)
    return

def add_to_queue(tableSize, num_consumers, task_queues, stop_flag, ident, f):
    bit_length = 86 
    b = Blake2b(output_bit_length=bit_length) 
    leftKey = Blake2b(output_bit_length=80, salt=b'left') 
    rightKey = Blake2b(output_bit_length=80, salt=b'right')
    bins_per_process = tableSize // num_consumers
    jobs_done = 0
    while not stop_flag.is_set():
        prime = b.PrimeHash(token_urlsafe(16).encode('utf-8'))
        KeyLeft = leftKey.KeyGen(str(prime).encode('utf-8'), tableSize)
        KeyRight = rightKey.KeyGen(str(prime).encode('utf-8'), tableSize)
        
        while True:
            try:
                task_queues[KeyLeft//bins_per_process].put([prime, KeyLeft % bins_per_process], timeout=1) 
                task_queues[KeyRight//bins_per_process].put([prime, KeyRight % bins_per_process], timeout=1) 
                break
            except Full:
                if stop_flag.is_set():
                    break  
                continue
            
        jobs_done += 1
    
    print(f'Producer: {ident} completed: {jobs_done} jobs', flush=True, file=f)
    return

if __name__ == '__main__':
    num_cpus = cpu_count()
    num_consumers = 57 #alter according to test
    num_producers = 10 #alter according to test
    table_size = num_consumers*8 
    NSize = 3072

    with open("qTests.txt", "a") as f:
        table = [[getrandbits(NSize), randint(2, 2**NSize - 1)] for _ in range(table_size)]
        bins_lst = [table[(i * len(table)) // num_consumers:((i + 1) * len(table)) // num_consumers] for i in range(num_consumers)]

        task_queues = [JoinableQueue(maxsize=1000) for _ in range(num_consumers)]

        event = Event()

        producers = [Process(target=add_to_queue, args=(table_size, num_consumers, task_queues, event, i, f)) for i in range(num_producers)]
        consumers = [Process(target=task, args=(bins_lst[queue_num], queue, event, queue_num, f)) for queue_num, queue in enumerate(task_queues, start=0)]

        # Start all processes
        for p in producers + consumers:
            p.start()

        # Run for 1 hour
        time.sleep(3600)

        event.set()

        # close all all processes once they terminate
        time.sleep(180)
        for p in producers:
            if p.is_alive():
                print("Terminating child process")
                p.terminate()
            p.join()
        for p in consumers:
            p.join()

        print('Number of CPUS: ', num_cpus, file=f)
        print('number of producers: ', num_producers, '  Number of consumers: ', num_consumers, file=f)
        print('number of bins total: ', table_size, file=f)
        print('size of N: ', NSize, file=f)
    
    print('Test Complete')
