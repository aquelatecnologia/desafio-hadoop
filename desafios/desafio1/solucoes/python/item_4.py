from mrjob.job import MRJob
from mrjob.step import MRStep

class MediaInstallPorCategoria(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (app, category, rating, reviews, size, installs, type, price, content_rating, gender, last_update, current_ver, android_ver) = line.split(';')
        yield category, installs

    def reducer(self, key, values):
        count = 0
        _sum = 0

        for value in values:
            count += 1
            _sum += int(value)
        
        avg = _sum/count

        yield key, avg

if __name__ == '__main__':
    MediaInstallPorCategoria.run()
