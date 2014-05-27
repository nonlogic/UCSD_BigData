#!/usr/bin/python
"""
PCA
"""
import sys
import numpy as np
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr
import pickle

class MRPCA(MRJob):
    def configure_options(self):
        super(MRPCA,self).configure_options()
        self.add_file_option('--buckets')

    def reducer_init(self):
        f = gzip.open( self.options.buckets, "rb" )
        pickleFile = cPickle.Unpickler( f )
        self.buckets = pickleFile.load()#[['latitude','longitude','elevation']]
        f.close()
    
    def mapper_concatenate(self, _, line):
        #buckets = pickle.load( open( "buck/buckets.pkl", "rb" ) )
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            number_defined=sum([e!='' for e in elements[3:]])
            if elements[0] != 'station' and number_defined == 365 and elements[1] in ['TMAX', 'TMIN']:
                yield ([elements[0],elements[2]],[1]+elements[:])

        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)            

    def reducer_concatenate(self, station_year, counts):
        #buckets = pickle.load( open( "buck/buckets.pkl", "rb" ) )
        self.increment_counter('MrJob Counters','reducer',1)
        new=[]
        for v in counts:
            if v[2]=='TMAX':
                new=v[4:]+new
            else:
                new=new+v[4:]
        tmp='1'
        if len(new)==730:
            for s in new:
                tmp=tmp+' '+s
            tmp=tmp+' 1'
            yield (self.buckets[station_year[0]],[tmp])
            
    def steps(self):
        return [
            self.mr(mapper=self.mapper_concatenate,
                    reducer=self.reducer_concatenate)#,
            #self.mr(reducer=self.reducer_find_max_word)
        ]    

if __name__ == '__main__':
    MRPCA.run()