#!/usr/bin/python
"""
collect the statistics for each station.
"""
import re,pickle,base64,zlib
from sys import stderr
import sys

sys.path.append('/usr/lib/python2.6/dist-packages') # a hack because anaconda made mrjob unreachable
from mrjob.job import MRJob
from mrjob.protocol import *

import traceback
from functools import wraps
from sys import stderr

"""this decorator is intended for decorating a function, not a
generator.  Therefor to use it in the context of mrjob, the generator
should call a function that handles a single input records, and that
function should be decorated.

The reason is that if a generator throws an exception it exits and
cannot process any more records.

"""
def ECatch(func):
    f_name=func.__name__
    @wraps(func)
    def inner(self,*args,**kwargs):
        try:
            self.increment_counter(self.__class__.__name__,'total in '+f_name,1)
            return func(self,*args,**kwargs)
        except Exception as e:
            self.increment_counter(self.__class__.__name__,'errors in '+f_name,1)
            stderr.write('Error:')
            stderr.write(str(e))
            traceback.print_exc(file=stderr)
            stderr.write('Arguments were %s, %s\n'%(args,kwargs))
            pass
    return inner        

"""
Functions for encoding and decoding arbitrary object into ascii 
so that they can be passed through the hadoop streaming interface.
"""

def loads(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def dumps(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))

class MRWeather(MRJob):

    @ECatch
    def map_one(self,line):
        return line.split(',')
    
    def mapper(self, _, line):
        elements=self.map_one(line)
        yield(elements[0],elements[1:])
            
    def check_integrity(self,meas,year,length):
        year=int(year)
        if year<1000 or year > 2014: return False
        if meas=='': return False
        if length != 367: return False
        return True
    
    @ECatch
    def reduce_one(self,S,vector):
        meas=vector[0]
        year=vector[1]
        length=len(vector)
        number_defined=sum([e!='' for e in vector[2:]])
        assert self.check_integrity(meas,year,length)==True
        S[(meas,int(year))]=number_defined
        
    def reducer(self, station, vectors):
        S={}
        for vector in vectors:
            self.reduce_one(S,vector)
        yield(station,dumps(S))
                              
if __name__ == '__main__':
    MRWeather.run()