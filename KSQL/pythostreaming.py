import json

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
from pyspark import sql
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/praveen/Downloads/spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar,/home/praveen/Downloads/spark-streaming-kafka-0-8_2.11-2.4.5.jar pyspark-shell'


def process_rdd(rdd):
    print(rdd)
    df =rdd.collect()
    data=sc.parallelize(df).collect()
    print(type(data))
    for every in data.count(data):
        data = sqlContext.createDataFrame(sc.parallelize(every))
        data.show()
    # print(type(df))
    # # for every in df:
    # data=rdd.map(Row(lambda x:x[1]))
    # df1 = data.collect()
    # print(df1)




if __name__=="__main__":
    conf = SparkConf().setAppName("StreamingDirectKafka")
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, 5)
    sqlContext = sql.SQLContext(sc)
    topic = ['test']
    kafkaParams = {"metadata.broker.list": "localhost:9094"}
    try:
        lines = KafkaUtils.createDirectStream(ssc, topic, kafkaParams)
        print(lines)
        data = lines.map(lambda x: json.loads(x[1]))

        # new_data = data.map(lambda x:str(x))
        #
        # print(data)
        data.foreachRDD(process_rdd)
    except Exception as e:
        pass
    ssc.start()
    ssc.awaitTermination()