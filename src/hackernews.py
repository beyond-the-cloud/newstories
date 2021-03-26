import requests, json
from kafka import KafkaProducer
import logging
import sys
from prometheus_client import (
  push_to_gateway,
  start_http_server,
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
PUSH_GATEWAY = "prometheus-prometheus-pushgateway:9091"
REGISTRY = CollectorRegistry()

KAFKA_COUNTER = Gauge("newstories_kafka_count", "Count of newstories sending message to kafka")
REQUEST_TIME = Summary('newstories_kafka_duration', 'Time newstories kafka producer spent sending messages')
COUNTER = Counter('newstories_hackernews_counter', 'Count of newstories visiting HackerNews')

# You need to register all of the metrics with your registry.  I like doing it
# this way, but you can also pass the registry when you create your metrics.
REGISTRY.register(KAFKA_COUNTER)
REGISTRY.register(REQUEST_TIME)
REGISTRY.register(COUNTER)


def count_kafka(f):
    def kafka_count(*args, **kwargs):
        result = f(*args, **kwargs)
        for k,v in result.iteritems():
            KAFKA_COUNTER.labels(device_type=k).set(v)
        push_to_gateway(PUSH_GATEWAY, job='pushgateway', registry=REGISTRY)
        return result
    return kafka_count


# Decorate function with metric.
@REQUEST_TIME.time()
@count_kafka
def send_id_to_kafka(id):
    producer.send('newstories', str(id).encode())
    # add logs
    logger.info("kafka producer sending newstories id: " + str(id))
    producer.flush()

if __name__ == '__main__':
  # get ids from hackernews
  response = requests.get('https://hacker-news.firebaseio.com/v0/newstories.json?print=pretty')
  ids = json.loads(response.text)
  # Increment by 1
  COUNTER.inc()
  push_to_gateway(PUSH_GATEWAY, job='pushgateway', registry=REGISTRY)
  # add logs
  logger.info("got newstories ids from HackerNews")

  # Start up the server to expose the metrics.
  start_http_server(8080)

  # send id with kafka producer
  for id in ids:
    send_id_to_kafka(id)