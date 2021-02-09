import sys, re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Wrod Counts')
sc = SparkContext(conf=conf)

SPARK_HOME = '/Users/isang-geon/spark'

# 명령 줄 인수를 확인한다.
if(len(sys.argv) != 3):
    print("""\
This program will count occurrences of each word in a document or documents
and return the counts sorted by the most frequently occurring words

Usage: wordcounts.py SPARK_HOME/data/shakespeare.txt /Users/isang-geon/Desktop/works/pysparkes""")
    sys.exit(0)
else:
    inputpath = sys.argv[1]
    outputdir = sys.argv[2]
    
# 단어 카운트 및 정렬
wordcounts = sc.textFile("file://" + inputpath) \
                .filter(lambda line: len(line) > 0) \
                .flatMap(lambda line: re.split('\W+', line)) \
                .filter(lambda word: len(word) > 0) \
                .map(lambda word:(word.lower(), 1)) \
                .reduceByKey(lambda v1, v2: v1 + v2) \
                .map(lambda x:(x[1],x[0])) \
                .sortByKey(ascending=False) \
                .persist()
wordcounts.saveAsTextFile("file://" + outputdir)
top5words = wordcounts.take(5)
justwords = []
for wordsandcounts in top5words:
    justwords.append(wordsandcounts[1])
print("The top five words are : " + str(justwords))
print("Check the complete output in " + outputdir)