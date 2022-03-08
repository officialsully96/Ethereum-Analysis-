from mrjob.job import MRJob
from datetime import datetime
class gas(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                block_timestamp = int(fields[6])
                date = time.strftime("%m/%Y", time.gmtime(block_timestamp))
                gas_price = int(fields[5])
                yield (date, gas_price)
        except:
            pass

    def combiner(self, key, value):
        yield (key, sum(value))

    def reducer(self, key, value):
        yield (key, sum(value))


if __name__ == '__main__':
    gas.run()