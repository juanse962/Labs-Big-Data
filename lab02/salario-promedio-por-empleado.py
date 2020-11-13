from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):



    def mapper(self, _, line):
#       for w in line.decode('utf-8', 'ignore').split():
        vec=  line.split(',')
        se= vec[0]
        sa= int(vec[1])
        yield se,sa



    def reducer(self, key, values):
        yield key, sum(values)



if __name__ == '__main__':
    MRWordFrequencyCount.run()
