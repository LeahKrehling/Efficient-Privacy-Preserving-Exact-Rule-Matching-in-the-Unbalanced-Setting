import hashlib
import sys
import os
import traceback
import itertools
import math
import random
import time
import gmpy2
import pickle
from gmpy2 import mpz
from numpy.random import seed
from numpy.random import randint

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

        digest_size = math.ceil(output_bit_length / 8) # convert to bytes

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
            candidate = gmpy2.bit_set(self.output_modulus + U, 0)

            if gmpy2.is_prime(candidate, 20):
                return candidate

            h.update(b'0') 
            current_digest = h.digest()

    def KeyGen(self, preimage, size):
        h = self.hash.copy()
        h.update(preimage)
        digest = h.hexdigest()
        return int(digest, 16) % size

class HashTable(object):
    def __init__(self, size):
        self.table = [[] for i in range(size)]
        self.PQTable = [[] for i in range(size)] 
        self.size = size

    def SaveContents(self, name):
        fileName = "%s.pkl" % name
        with open(fileName, "wb") as file:
            pickle.dump(self.table, file)
        
        secondFileName = "PQ%s.pkl" %name
        with open(secondFileName, "wb") as file:
            pickle.dump(self.PQTable, file)

    def AddValues(self, key, value):
        if key >= self.size:
            raise ValueError("Key outside of table limits")
        self.table[key].append(value)
    
    def SizeOfBin(self, key):
        if key >= self.size:
            raise ValueError("Key outside of table limits")
        return len(self.table[key])

    def RSAObject(self, padPrimeList):
        for key in range(len(self.table)):
            setP, setQ = self.GetPQset(self.table[key])
            P = mpz(math.prod(setP)) 
            Q = mpz(math.prod(setQ))

            P, Ppad = self.FindPad(padPrimeList, P)
            Q, Qpad = self.FindPad(padPrimeList, Q)

            self.PQTable[key].append(P)
            self.PQTable[key].append(Q)

            setP.append(Ppad)
            setQ.append(Qpad)

            self.PQTable[key].append(setP)
            self.PQTable[key].append(setQ)

            g = self.FindG(setP, setQ, P, Q)
            self.table[key].clear()
            self.table[key].append(g)
            self.table[key].append(mpz(P*Q))

    def GetPQset(self, bin):
        setP = []
        setQ = []
        for i in range(len(bin)):
            if i % 2 == 0:
                setP.append(bin[i])
            else:
                setQ.append(bin[i])
        return setP, setQ

    def FindPad(self, padPrimeList, smallPrimes):
        while True:
            pad = padPrimeList[i]
            candidate = mpz(2 * smallPrimes * pad + 1)
            if gmpy2.is_prime(candidate, 40):
                return candidate, pad

    def MakeG(self, P, Q):
        N = mpz(P*Q)
        H = mpz(random.randint(2, N))
        g = gmpy2.powmod(H, 2, N)
        return g

    def FindG(self, setP, setQ, P, Q):
        g = self.MakeG(P, Q)
        for (primeP, primeQ) in itertools.zip_longest(setP, setQ, fillvalue=None):
            if primeP == None:
                xq = gmpy2.powmod(g, int((Q-1)//primeQ), Q)
            elif primeQ == None:
                xp = gmpy2.powmod(g, int((P-1)//primeP), P)
            else:
                xp = gmpy2.powmod(g, int((P-1)//primeP), P)
                xq = gmpy2.powmod(g, int((Q-1)//primeQ), Q)
            if xp == 1 or xq == 1:
                g = self.FindG(setP, setQ, P, Q)
        return g


if __name__ == "__main__":
    tableSize = 902
    leftTable = HashTable(tableSize)
    rightTable = HashTable(tableSize)

    sigSize = 10000 # The size of the receiver's set
    seed(int.from_bytes(os.urandom(4), byteorder=sys.byteorder))
    values = randint(0, 2000000, size=sigSize) 
    
    bit_length = 86 #The security parameter
    b = Blake2b(output_bit_length=bit_length) # hash object for primes
    leftKey = Blake2b(output_bit_length=80, salt=b'left') # alter based on size of table
    rightKey = Blake2b(output_bit_length=80, salt=b'right')

    # Load in list of primes of requisit size
    with open("primes.pkl", "rb") as file:
        padPrimeList = pickle.load(file)

    start = time.perf_counter()
    for i in values:
        prime = b.PrimeHash(str(i).encode('utf-8'))
        KeyLeft = leftKey.KeyGen(str(prime).encode('utf-8'), tableSize)
        KeyRight = rightKey.KeyGen(str(prime).encode('utf-8'), tableSize)
        sizeLeft = leftTable.SizeOfBin(KeyLeft)
        sizeRight = rightTable.SizeOfBin(KeyRight)

        if sizeLeft == sizeRight:
            leftTable.AddValues(KeyLeft, prime)
        elif sizeLeft < sizeRight:
            leftTable.AddValues(KeyLeft, prime)
        elif sizeRight < sizeLeft:
            rightTable.AddValues(KeyRight, prime)
    print('Time to find', sigSize, 'primes and sort into', 2*tableSize, 'bins:', time.perf_counter() - start)
    
    start = time.perf_counter()

    leftTable.RSAObject(padPrimeList)
    rightTable.RSAObject(padPrimeList)

    print('Time to find N and g for each bin:', time.perf_counter() - start)

    leftTable.SaveContents("leftTable")
    rightTable.SaveContents("rightTable")
  
