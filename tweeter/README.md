# Tweeter

Stream tweets via Twitter API tracking keywords.

## Set up

Create an app account on https://apps.twitter.com. Export the following variables:

```bash
export TWITTER_CONSUMER_KEY=
export TWITTER_CONSUMER_SECRET=
export TWITTER_ACCESS_TOKEN=
export TWITTER_ACCESS_TOKEN_SECRET=
```

## Running

Assuming you have Kafka running, just call:

```bash
make run
```
