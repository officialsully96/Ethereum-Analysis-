from mrjob.job import MRJob
from datetime import datetime
import time


class Part1a(MRJob):
    def mapper(self, _, line):

        try:
            fields = line.split(',')
            block_timestamp = int(fields[6])
            date = time.strftime("%m/%Y", time.gmtime(block_timestamp))
            yield (date, 1)
        except:
            pass


    def combiner(self, key, values):
        yield (key, sum(values))

    def reducer(self, key, values):
        yield (key, sum(values))


if __name__ == '__main__':
    Part1a.run()
