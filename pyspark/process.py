from __future__ import print_function

import os
import sys
import json
import tempfile

from pyspark import Row
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

IS_PY2 = sys.version_info < (3,)

config = {
    k: os.getenv(k, default)
    for k, default in [
        ('KAFKA_BROKERS', '0.0.0.0:9092'),
        ('KAFKA_CONSUMER_GROUP', 'python_consumer'),
        ('KAFKA_CLIENT_ID', 'client_1'),
        ('KAFKA_TOPICS', 'twitter'),
        ('SPARK_APP_NAME', 'TwitterStreamKafka'),
        ('STREAM_BATCH_DURATION', 1),  # seconds
        ('STREAM_CONTEXT_TIMEOUT', 60),  # seconds
        ('PYSPARK_PYTHON', 'python' if IS_PY2 else 'python3'),
    ]
}


CHECKPOINT = tempfile.mkdtemp()
KAFKA_PARAMS = {'metadata.broker.list': config['KAFKA_BROKERS']}
SPARK_CONF = (SparkConf()
              .setMaster('local[2]')
              .setAppName(config['SPARK_APP_NAME']))

offsetRanges = []


def get_session(spark_conf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = (SparkSession
                                                      .builder
                                                      .config(conf=spark_conf)
                                                      .enableHiveSupport()
                                                      .getOrCreate())
    return globals()['sparkSessionSingletonInstance']


def create_context():
    spark = get_session(SPARK_CONF)
    ssc = StreamingContext(spark.sparkContext, config['STREAM_BATCH_DURATION'])
    ssc.checkpoint(CHECKPOINT)
    # start offsets from beginning
    # won't work if we have a chackpoint
    offsets = {TopicAndPartition(topic, 0): 0 for topic in config['KAFKA_TOPICS']}
    stream = KafkaUtils.createDirectStream(ssc, config['KAFKA_TOPICS'], KAFKA_PARAMS, offsets)
    main(stream)
    return ssc


def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


def printOffsetRanges(rdd):
    for o in offsetRanges:
        print("Offset range: %s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset))


def get_hashtags(tweet):
    return [hashtag['text'] for hashtag in tweet['entities']['hashtags']]


def process(timestamp, rdd):
    try:
        # Get the singleton instance of SparkSession
        spark = get_session(rdd.context.getConf())

        # Convert RDD[List[String]] to RDD[Row] to DataFrame
        rows = rdd.flatMap(lambda a: a).map(lambda w: Row(word=w))

        words_df = spark.createDataFrame(rows)

        # Creates a temporary view using the DataFrame
        words_df.createOrReplaceTempView('words')

        # Do word count on table using SQL and print it
        sql = "SELECT word, COUNT(1) AS total FROM words GROUP BY word"
        word_count_df = spark.sql(sql)
        word_count_df.show()
    except Exception:
        pass


def main(stream):
    tweets = stream.map(lambda x: json.loads(x[1]))

    # list the most common works using a running count
    running_count = (tweets
                     .flatMap(lambda tweet: tweet['text'].split(' '))
                     .countByValue()
                     .updateStateByKey(updateFunc)
                     .transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
                     .map(lambda x: "%s (%s)" % x))

    running_count.pprint()

    # Count number of tweets in the batch
    count_batch = (tweets
                   .window(windowDuration=60, slideDuration=5)
                   .count()
                   .map(lambda x: ('Num tweets: %s' % x)))

    # Count by windowed time period
    count_windowed = (tweets
                      .countByWindow(windowDuration=60, slideDuration=5)
                      .map(lambda x: ('Tweets total (1-min rolling): %s' % x)))

    count_batch.union(count_windowed).pprint()

    (stream
     .transform(storeOffsetRanges)
     .foreachRDD(printOffsetRanges))

    (tweets
     .map(get_hashtags)
     .foreachRDD(process))


if __name__ == '__main__':
    spark = get_session(SPARK_CONF)
    spark.sparkContext.setLogLevel('WARN')  # suppress spark logging

    ssc = StreamingContext.getOrCreate(CHECKPOINT, create_context)
    ssc.start()
    ssc.awaitTermination(timeout=config['STREAM_CONTEXT_TIMEOUT'])
    ssc.stop()
