from datetime import datetime
from mrjob.job import MRJob
import time


class partB(MRJob):
    def mapper(self, _, line):

        try:
            fields = line.split(',')
            value = int(fields[3])
            date = int(fields[6])
            purchaseDate = time.strftime("%m/%Y", time.gmtime(date))
            yield (purchaseDate, (value, 1))
        except:
            pass

    def combiner(self, key, values):

        total = 0
        count = 0

        for value in values:
            count += value[1]
            total += value[0]

        yield (key, (total, count))

    def reducer(self, key, values):

        total = 0
        count = 0

        for value in values:
            count += value[1]
            total += value[0]

        yield (key, total / count)


if __name__ == '__main__':
    partB.run()
