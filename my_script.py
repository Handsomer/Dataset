#coding:utf-8

from pyspark import SparkConf, SparkContext

def containsSpark(s):
    return "Spark" in s

if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf = conf)
    #创建 一个字符串的RDD
    lines = sc.textFile("/opt/module/spark/README.md")
    #RDD 转化
    python_line = lines.filter(lambda line: "Spark" in line)
    #获取第一个结果
    print(python_line.first(), '--- --- ---\n')
    #计算总行数
    print(python_line.count(), '---- ---- ----\n')
    #创建RDD
    #该sc 只可初始化一次
    lines1 = sc.parallelize(["pandas", "i like pandas"])
    print(lines1.count(), '---- line count')
    #PDD 操作，转化操作
    spark_rdd = lines.filter(lambda x: "Spark" in x)
    for_rdd = lines.filter(lambda x: "for" in x)
    s_f_rdd = spark_rdd.union(for_rdd)
    print(s_f_rdd.count(), '--- --- --- spark for rdd count')
    print(spark_rdd.count(), '--- --- --- spark rdd count')
    print(for_rdd.count(), '--- --- --- for rdd count', type(for_rdd.count()))
    #获取部分少量元素
        ##拿出10个元素并输出
    num = 0
    for line in  s_f_rdd.take(10):
        num += 1
        print(line,'+++++ +++++++ +++++ take', num)
        #获取全部元素
    num = 0
    for line in  s_f_rdd.collect():
        num += 1
        print(line,'+++++ +++++++ +++++ collect', num)
    word = s_f_rdd.filter(containsSpark)
    print(word.count(), '-------- -------')
