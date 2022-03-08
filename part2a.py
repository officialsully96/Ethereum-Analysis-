from datetime import datetime
from mrjob.job import MRJob
import time


class part2a(MRJob):
    def mapper(self, _, line):

        try:
            fields = line.split(',')
            value = int(fields[3])
            to_adress = fields[2]
            if value == 0:
                pass
            else:
                yield (to_adress, value)
        except:
            pass

    def combiner(self, value, counts):
        yield (value, sum(counts))

    def reducer(self, value, counts):
        yield (value, sum(counts))

if __name__ == '__main__':
    part2a.run()
