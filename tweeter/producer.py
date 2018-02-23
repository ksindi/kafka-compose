import os

import tweepy
from confluent_kafka import Producer

config = {
    k: os.getenv(k, default)
    for k, default in [
        ('KAFKA_BROKERS', '0.0.0.0:9092'),
        ('KAFKA_TOPICS', 'twitter'),
        ('TWITTER_CONSUMER_KEY', None),
        ('TWITTER_CONSUMER_SECRET', None),
        ('TWITTER_ACCESS_TOKEN', None),
        ('TWITTER_ACCESS_TOKEN_SECRET', None),
        ('TWITTER_RATE_LIMIT', 100),
        ('TWITTER_KEYWORDS', 'python'),
    ]
}


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(TwitterStreamListener, self).__init__()
        self.producer = Producer(**{'bootstrap.servers': config['KAFKA_BROKERS']})
        self.count = 0
        self.tweets = []

    def on_data(self, data):
        self.producer.produce(config['KAFKA_TOPICS'], data.encode('utf-8'))
        self.producer.flush()
        print('Tweet count: ', self.count)
        self.count += + 1
        return self.count <= config['TWITTER_RATE_LIMIT']

    def on_error(self, status_code):
        if status_code == 420:  # rate limit
            return False


if __name__ == '__main__':
    auth = tweepy.OAuthHandler(config['TWITTER_CONSUMER_KEY'], config['TWITTER_CONSUMER_SECRET'])
    auth.set_access_token(config['TWITTER_ACCESS_TOKEN'], config['TWITTER_ACCESS_TOKEN_SECRET'])
    api = tweepy.API(auth)

    listener = TwitterStreamListener()
    twitter_stream = tweepy.Stream(auth=api.auth, listener=listener)
    twitter_stream.filter(track=config['TWITTER_KEYWORDS'].split(','), languages=['en'], async=True)
