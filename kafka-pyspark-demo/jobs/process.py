#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Spark Streaming Twitter.

spark-submit \
  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
  process.py \
  --zooke

"""
from __future__ import print_function

import os
import sys
import json
import argparse

from pyspark import Row
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

IS_PY2 = sys.version_info < (3,)
APP_NAME = 'TwitterStreamKafka'
BATCH_DURATION = 1  # in seconds
ZK_QUORUM = 'localhost:32181'
GROUP_ID = 'spark-streaming-consumer'
TOPICS = ['twitter']
CHECKPOINT = '/tmp/%s' % APP_NAME
STREAM_CONTEXT_TIMEOUT = 60  # seconds
KAFKA_PARAMS = {"metadata.broker.list": 'localhost:29092'}
SPARK_CONF = (SparkConf()
              .setMaster('local[2]')
              .setAppName(APP_NAME))

offsetRanges = []

if not IS_PY2:
    os.environ['PYSPARK_PYTHON'] = 'python3'


def create_parser():
    parser = argparse.ArgumentParser(description=APP_NAME)
    return parser


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
    ssc = StreamingContext(spark.sparkContext, BATCH_DURATION)
    ssc.checkpoint(CHECKPOINT)
    # start offsets from beginning
    # won't work if we have a chackpoint
    offsets = {TopicAndPartition(topic, 0): 0 for topic in TOPICS}
    stream = KafkaUtils.createDirectStream(ssc, TOPICS, KAFKA_PARAMS, offsets)
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
        print("Offset range: %s %s %s %s" %
              (o.topic, o.partition, o.fromOffset, o.untilOffset))


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
    except:
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

    # Get authors
    authors = tweets.map(lambda tweet: tweet['user']['screen_name'])

    count_batch.union(count_windowed).pprint()

    (stream
     .transform(storeOffsetRanges)
     .foreachRDD(printOffsetRanges))

    (tweets
     .map(get_hashtags)
     .foreachRDD(process))


if __name__ == '__main__':
    import shutil; shutil.rmtree(CHECKPOINT)  # delete any checkpoints

    parser = create_parser()
    args = parser.parse_args()
    print('Args: ', args)

    spark = get_session(SPARK_CONF)
    spark.sparkContext.setLogLevel('WARN')  # suppress spark logging

    ssc = StreamingContext.getOrCreate(CHECKPOINT, create_context)
    ssc.start()
    ssc.awaitTermination(timeout=STREAM_CONTEXT_TIMEOUT)
    ssc.stop()
