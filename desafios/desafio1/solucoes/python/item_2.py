from mrjob.job import MRJob
from mrjob.step import MRStep

class AppByInstall(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (app, category, rating, reviews, size, installs, type, price, content_rating, gender, last_update, current_ver, android_ver) = line.split(';')
        yield "%010d" % int(reviews), app

    def reducer_count_ratings(self, key, values):
        yield int(key), ";".join(values)

if __name__ == '__main__':
    AppByInstall.run()
