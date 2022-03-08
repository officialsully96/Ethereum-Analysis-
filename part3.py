from mrjob.job import MRJob
from mrjob.step import MRStep

class Part3(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 9 :
                miner = fields[2]
                size = int(fields[4])
                yield (miner, size)

            else:
                pass

        except:
            pass


    def reducer_1(self, key , size):
        yield (None,(key,sum(size))


    def reducer_2(self,key ,value):
        sorted_values = sorted(v, reverse=True, key=lambda tup: tup[1])
        i = 0
        for value in sorted_values:
            yield (key, value)
            i += 1
            if i >= 10:
                break



    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer_1=self.reducer_1),
                MRStep(reducer=self.reducer_2)]


if __name__ == '__main__':
    Part3.run()
