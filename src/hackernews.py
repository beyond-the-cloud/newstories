import requests, json
from kafka import KafkaProducer
import logging
import sys
from prometheus_client import (
  push_to_gateway,
  Counter,
  CollectorRegistry,
  Gauge,
  Summary,
)


# set log
logger = logging.getLogger("newstories-logger")
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# init kafka producer
producer = KafkaProducer(bootstrap_servers='kafka-0.kafka-headless.default.svc.cluster.local:9092')

# Create a metric to track time spent and requests made.
PUSH_GATEWAY = "http://prometheus-prometheus-pushgateway:9091"
REGISTRY = CollectorRegistry()

KAFKA_COUNTER = Counter("newstories_kafka_counter", "Count of newstories sending message to kafka")
REQUEST_TIME = Summary('newstories_kafka_duration', 'Time newstories kafka producer spent sending messages')
COUNTER = Counter('newstories_hackernews_counter', 'Count of newstories visiting HackerNews')

# You need to register all of the metrics with your registry.  I like doing it
# this way, but you can also pass the registry when you create your metrics.
REGISTRY.register(KAFKA_COUNTER)
REGISTRY.register(REQUEST_TIME)
REGISTRY.register(COUNTER)


# Decorate function with metric.
@REQUEST_TIME.time()
def send_id_to_kafka(id):
    producer.send('newstories', str(id).encode())
    # add logs
    logger.info("kafka producer sending newstories id: " + str(id))
    KAFKA_COUNTER.inc()
    push_to_gateway(PUSH_GATEWAY, job='newstories_cronjob', registry=REGISTRY)
    producer.flush()


if __name__ == '__main__':
  # get ids from hackernews
  response = requests.get('https://hacker-news.firebaseio.com/v0/newstories.json?print=pretty')
  ids = json.loads(response.text)
  # increment by 1
  COUNTER.inc()
  # add logs
  logger.info("got newstories ids from HackerNews")
  # send id with kafka producer
  for id in ids:
    send_id_to_kafka(id)
  # push metrics to pushgateway
  push_to_gateway(PUSH_GATEWAY, job='newstories_cronjob', registry=REGISTRY)
