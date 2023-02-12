from mrjob.job import MRJob

class WordCountJob(MRJob):

    def mapper(self, _, line):
         for word in line.split():
             yield(word, 1)

    def reducer(self, key, values):
        # The reducer function takes in a word and its list of counts and outputs the sum
        yield key, sum(values)

if __name__ == '__main__':
    WordCountJob.run()



