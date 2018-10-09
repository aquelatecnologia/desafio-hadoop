from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.sort_mapper, reducer=self.sort_reducer)
        ]

    def mapper(self, _, line):
        words = line.split(' ')
        for word in words:
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)

    def sort_mapper(self, word, count):
        yield "%05d" % count, word

    def sort_reducer(self, count, words):
        yield count, " , ".join(words)

if __name__ == '__main__':
    WordCount.run()
