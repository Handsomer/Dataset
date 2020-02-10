#coding:utf-8

from pyspark import SparkConf, SparkContext
from functools import reduce

def build_map(origin_list):
    return list(map(lambda x: (x, 1), origin_list))

def build_reduce(origin_map):
    sum_num =reduce(lambda x, y : (x[0] + y[0], x[1] + y[1]), origin_map)
    return sum_num

#origin_list : [1,2,3,4]
def getAVGByMapReduce(origin_list):
    ret_map = build_map(origin_list)
    sums, nums = build_reduce(ret_map)
    return sums, nums

def getAVGBySparkMapReduce(origin_list):
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf = conf)
    nums = sc.parallelize(origin_list)
    map_list = nums.map(lambda x: (x, 1))
    return map_list.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

def getAVGBySparkAggregate(origin_list):
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf = conf)
    nums = sc.parallelize(origin_list)
    sumCount = nums.aggregate((0, 0),(lambda acc, value: (acc[0] + value, acc[1] + 1)), (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])))
    return sumCount

if __name__ == "__main__":
    #print(getAVGByMapReduce([1,2,3,4]))
    #ret_values = getAVGBySparkMapReduce([1,2,3,4])
    ret_values = getAVGBySparkAggregate([1,2,3,4])
    print(ret_values, '--- --- --- --- --- ---')
