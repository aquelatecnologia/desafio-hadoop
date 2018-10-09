from mrjob.job import MRJob
from mrjob.step import MRStep

import re

class MediaTamanhoPorCategoria(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (app, category, rating, reviews, size, installs, type, price, content_rating, gender, last_update, current_ver, android_ver) = line.split(';')
        tmp = re.split(r'([0-9.]+)', size)
            
        if (len (tmp) == 3):                        
            yield category, (float(tmp[1]) * (1024 if tmp[2] == 'k' else 1024*1024))

    def reducer(self, key, values):
        count = 0
        _sum = 0

        for value in values:
            count += 1
            _sum += value
        
        avg = _sum/count

        letter = 0
        letters = ['', 'k', 'M']

        while (avg > 1024):
            avg /= 1024
            letter += 1
        

        yield key, str(int(avg)) + ' ' + letters[letter]

if __name__ == '__main__':
    MediaTamanhoPorCategoria.run()
