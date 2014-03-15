from multiprocessing import Pool 
import sys

def findAndParse():
	


if __name__ == '__main__':
	numP = sys.argv[1]
	pool = Pool(processes=numP)
	result = apply_async(findAndParse)
