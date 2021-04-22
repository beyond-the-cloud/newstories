import os
import requests, json
from kafka import KafkaProducer
import logging
import sys
import time
from prometheus_client import (
  push_to_gateway,
  CollectorRegistry,
  Summary,
)

# set log
logger = logging.getLogger("newstories-logger")
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# init kafka producer
kafka_server = os.environ.get('KAFKA_HOST') + ':' + os.environ.get('KAFKA_PORT')
producer = KafkaProducer(bootstrap_servers=kafka_server)

# test if Istio SideCar is ready
while requests.get('http://localhost:15021/healthz/ready').status_code != 200:
  logger.info("Waiting for SideCar get ready...")
  time.sleep(1)

logger.info("SideCar is running, start getting stories")

# Create a metric to track time spent and requests made.
PUSH_GATEWAY = "http://prometheus-prometheus-pushgateway.default:9091"
REGISTRY = CollectorRegistry()
REQUEST_TIME = Summary('newstories_kafka_duration', 'Time newstories kafka producer spent sending messages')
REGISTRY.register(REQUEST_TIME)


# Decorate function with metric.
@REQUEST_TIME.time()
def send_id_to_kafka(id):
    producer.send('newstories', str(id).encode())
    # add logs
    logger.info("kafka producer sending newstories id: " + str(id))
    push_to_gateway(PUSH_GATEWAY, job='newstories_cronjob', registry=REGISTRY)
    producer.flush()


if __name__ == '__main__':
  # get ids from hackernews
  response = requests.get('https://hacker-news.firebaseio.com/v0/newstories.json?print=pretty')
  ids = json.loads(response.text)
  # add logs
  logger.info("got newstories ids from HackerNews")
  # send id with kafka producer
  for id in ids:
    send_id_to_kafka(id)
  # push metrics to pushgateway
  push_to_gateway(PUSH_GATEWAY, job='newstories_cronjob', registry=REGISTRY)

  logger.info("Getting stories completed")
  # Stop Istio SideCar
  res = requests.post('http://localhost:15020/quitquitquit')
  logger.info("Stopping SiceCar: " + res.text)