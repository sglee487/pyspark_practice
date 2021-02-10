'''
이 연습에서는 체크포인트가 반복 루틴에 미칠 수 있는 영향을 보여준다.
'''
import sys
from pyspark import SparkConf, SparkContext
sc = SparkContext()
sc.setCheckpointDir("file:///Users/isang-geon/tmp/checkpointdir")
rddofints = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
try:
    # 이것은 rddofints에 대한 매우 긴 리니지를 만들 것이다.
    for i in range(1000):
        rddofints = rddofints.map(lambda x: x+1)
        if i % 10 == 0:
            print("Looped " + str(i) + " times")
            rddofints.checkpoint()
            rddofints.count()
except Exception as e:
    print("Exception: " + str(e))
    print("RDD Debug String: ")
    print(rddofints.toDebugString())
    sys.exit()
print("RDD Debug String: ")
print(rddofints.toDebugString())