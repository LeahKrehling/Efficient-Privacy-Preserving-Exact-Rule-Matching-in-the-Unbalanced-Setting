import hashlib
import math
import traceback
import sys
import time
from multiprocessing import Process, JoinableQueue, Queue, cpu_count
from gmpy2 import mpz, powmod, is_prime, bit_set
from random import randint, getrandbits
from secrets import token_urlsafe

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


def task(bins, task_queue, ident, f):
    while True:
        next_task = task_queue.get()
        if next_task is None:
            task_queue.task_done()
            break
        
        value, in_bin = next_task
        N, A = bins[in_bin]
        answer = powmod(A, value, N)
        bins[in_bin][1] = answer

        task_queue.task_done()
    return

def add_to_queue(tableSize, num_jobs, num_consumers, task_queues, ident, f):
    bit_length = 86 
    b = Blake2b(output_bit_length=bit_length) 
    leftKey = Blake2b(output_bit_length=80, salt=b'left') 
    rightKey = Blake2b(output_bit_length=80, salt=b'right')
    bins_per_process = tableSize // num_consumers
    for i in range(num_jobs):
        prime = b.PrimeHash(token_urlsafe(16).encode('utf-8'))
        KeyLeft = leftKey.KeyGen(str(prime).encode('utf-8'), tableSize)
        KeyRight = rightKey.KeyGen(str(prime).encode('utf-8'), tableSize)
        
        task_queues[KeyLeft//bins_per_process].put([prime, KeyLeft % bins_per_process])
        task_queues[KeyRight//bins_per_process].put([prime, KeyRight % bins_per_process]) 
    
    return

if __name__ == '__main__':
    num_cpus = cpu_count()
    NSize = 3072
    results = [] 

    with open("qTests.txt", "a") as f:
        for num_producers in range(1, 12): #alter according to desired tests
            for num_consumers in range(5, 49 - num_producers): #alter according to desired tests
                
                table_size = num_consumers*8 
                table = [[getrandbits(NSize), randint(2, 2**NSize - 1)] for _ in range(table_size)]
                bins_lst = [table[(i * len(table)) // num_consumers:((i + 1) * len(table)) // num_consumers] for i in range(num_consumers)]

                #creating the queues for the exponentiation units
                task_queues = [JoinableQueue(maxsize=1000) for _ in range(num_consumers)]

                # Creating and starting the prime hash processes
                ProdStart = time.perf_counter()
                num_jobs = 1000000//num_producers
                producers = []
                for i in range(num_producers):
                    producer = Process(target=add_to_queue, args=(table_size, num_jobs, num_consumers, task_queues, i, f))
                    producers.append(producer)
                    producer.start()

                #creating and starting all of the exponentiation processes
                ConStart = time.perf_counter()
                consumers = []
                for queue_num, queue in enumerate(task_queues, start=0):
                    process = Process(target=task, args=(bins_lst[queue_num], queue, queue_num, f))
                    consumers.append(process)
                    process.start()

                # Wait for all of the prime hash tasks to finish
                for process in producers:
                    process.join()

                time_prod = time.perf_counter()-ProdStart

                # Add a poison pill for each exponentiation unit
                for i in range(num_consumers):
                    task_queues[i].put(None)

                # Wait for all the EP to finish
                for process in consumers:
                    process.join()
                
                time_con = time.perf_counter()-ConStart

                results.append((num_producers, num_consumers, time_con, time_prod))
                print('Test:', num_producers, ':', num_consumers, 'complete')
            
            print('Tests: ',num_producers  , ' Complete')
        
        for result in results:
            print(f'Producers: {result[0]}, Consumers: {result[1]}, Con Time: {result[2]}, Prod Time: {result[2]}', file=f)
