from datetime import datetime
from mrjob.job import MRJob
import time

class part2b(MRJob):

    def mapper(self, _, line):
        try:
            if len(line.split(','))==5:
                fields = line.split(',')
                address = fields[0]
                block_number = fields[3]
                yield (address,(address,1))

            if len(line.split(',')) == 7:
                fields = line.split(',')
                to_address = fields[2]
                # to_address = to_address[1:-1]
                value = int(fields[3])
                yield (to_address, (value,2))
        except:
            pass
    def reducer(self, key, vals):
        block_number = None
        total = 0
        for val in vals:
            if val[1] == 1:
                block_number = val[0]
            if val[1] == 2:
                total = val[0]

        if block_number is None and total >= 0:
            yield (key, total)

if __name__ == '__main__':
    part2b.JOBCONF= { 'mapreduce.job.reduces': '15' }
    part2b.run()
