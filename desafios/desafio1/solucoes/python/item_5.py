from mrjob.job import MRJob
from mrjob.step import MRStep

import math
import re

class MelhorApp(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (app, category, rating, reviews, size, installs, type, price, content_rating, gender, last_update, current_ver, android_ver) = line.split(';')
        
        score = self.calc_score(float(installs))
        score += self.calc_score(float(reviews))
        score += self.calc_score(float(rating))

        yield category, (("%0.05f" % score) + " ; " + app)

    def reducer(self, key, values):
        yield key, " , ".join(values)

    def calc_score (self, value):
        if (math.isnan(value) or value == 0):
            return 1
        
        return (1 / value)

if __name__ == '__main__':
    MelhorApp.run()
