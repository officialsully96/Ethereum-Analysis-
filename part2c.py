from mrjob.job import MRJob
from mrjob.step import MRStep


class Part2c(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split()
            if len(fields) == 2:
                to_address = fields[0]
                value = int(fields[1])
                row = (to_address, value)
                yield (None, row)

        except:
            pass

    def combiner(self, key, lines):
        sorted_values = sorted(lines, reverse=True, key=lambda tup: tup[1])
        top = sorted_values[:10]

        for top_10 in top:
            yield (None, top_10)

    def reducer(self, key, lines):
        sorted_values = sorted(lines, reverse=True, key=lambda tup: tup[1])
        top = sorted_values[:10]

        for top_10 in top:
            yield (None, top_10)


if __name__ == '__main__':
    Part2c.run()
