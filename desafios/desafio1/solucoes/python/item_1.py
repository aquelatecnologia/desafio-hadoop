from mrjob.job import MRJob
from mrjob.step import MRStep

class ContarCategorias(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.sort_mapper, reducer=self.sort_reducer)
        ]

    def mapper(self, _, line):
        (app, category, rating, reviews, size, installs, type, price, content_rating, gender, last_update, current_ver, android_ver) = line.split(';')
        yield category, 1

    def reducer(self, category, values):
        yield category, sum(values)

    def sort_mapper(self, word, count):
        yield "%05d" % count, word

    def sort_reducer(self, count, words):
        yield count, " , ".join(words)

if __name__ == '__main__':
    ContarCategorias.run()
