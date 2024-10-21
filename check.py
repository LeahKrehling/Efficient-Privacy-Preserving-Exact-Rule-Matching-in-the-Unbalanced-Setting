import pickle
import time
from gmpy2 import mpz, powmod

if __name__ == "__main__":
    # Load in the tables
    start = time.perf_counter()
    tableSize = 715 # decided when the tables are made in generating code
    with open("leftTable.pkl", "rb") as file:
        leftTable = pickle.load(file)
    
    with open("rightTable.pkl", "rb") as file:
        rightTable = pickle.load(file)

    with open("PQleftTable.pkl", "rb") as file:
        PQLeftTable = pickle.load(file)
    
    with open("PQrightTable.pkl", "rb") as file:
        PQRightTable = pickle.load(file)

    SigCount = 0

    for key in range(len(leftTable)):
        g = mpz(leftTable[key][0])
        P = mpz(PQLeftTable[key][0])
        Q = mpz(PQLeftTable[key][1])
        setP = PQLeftTable[key][2]
        setQ = PQLeftTable[key][3]

        for i in setP:
            exponent = 2
            for k in setP:
                if k != i:
                    exponent = exponent*k
            test = powmod(g, exponent, P)
            if test == 1:
                SigCount = SigCount + 1
    
        for j in setQ:
            exponent = 2
            for m in setQ:
                if m != j:
                    exponent = exponent*m
            test = powmod(g, exponent, Q)
            if test == 1:
                SigCount = SigCount + 1

    for key in range(len(rightTable)):
        g = mpz(rightTable[key][0])
        P = mpz(PQRightTable[key][0])
        Q = mpz(PQRightTable[key][1])
        setP = PQRightTable[key][2]
        setQ = PQRightTable[key][3]

        for i in setP:
            exponent = 2
            for k in setP:
                if k != i:
                    exponent = exponent*k
            test = powmod(g, exponent, P)
            if test == 1:
                SigCount = SigCount + 1
    
        for j in setQ:
            exponent = 2
            for m in setQ:
                if m != j:
                    exponent = exponent*m
            test = powmod(g, exponent, Q)
            if test == 1:
                SigCount = SigCount + 1

    print ("the number of signatures hit was: ", SigCount)
    print('Time to check that was:', time.perf_counter() - start)
