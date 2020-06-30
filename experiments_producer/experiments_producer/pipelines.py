# -*- coding: utf-8 -*-
import uuid
import logging

import requests
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from scrapy.exceptions import NotConfigured

from experiments_producer.settings import KAFKA_SETTINGS


class KafkaProducerPipeline(object):
    """ An item pipeline that serializes the incoming items using Avro and
    produces this serialized data into a Kafka topic.
    """

    logger = logging.getLogger("KafkaProducerPipeline")

    def __init__(self, kafka_topic, kafka_key_schema, kafka_value_schema):
        print(kafka_key_schema)
        self._kafka_topic = kafka_topic
        self._kafka_key_schema = avro.loads(kafka_key_schema)
        self._kafka_value_schema = avro.loads(kafka_value_schema)
        self.producer = self._create_producer()

        # TODO: We should test if the kafka broker is alive and if the provided topic exists

    def _create_producer(self):
        bootstrap_servers = KAFKA_SETTINGS["bootstrap.servers"]
        schema_registry_url = KAFKA_SETTINGS["schema.registry.url"]

        self.logger.info("Creating the Kafka AvroProducer...")
        return AvroProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "on_delivery": self._on_delivery_callback,
                "schema.registry.url": schema_registry_url,
            }
        )

    def _on_delivery_callback(self, err, msg):
        if err is not None:
            self.logger.warning("Kafka message delivery failed: {}".format(err))
        else:
            self.logger.info(
                "Kafka message delivered to {} [{}]".format(
                    msg.topic(), msg.partition()
                )
            )

    @classmethod
    def from_crawler(cls, crawler):

        # Check if export to kafka is enabled at the spider
        kafka_export_enabled = getattr(crawler.spider, "kafka_export_enabled", False)
        if not kafka_export_enabled:
            raise NotConfigured

        # Load the kafka topic settings
        kafka_topic = getattr(crawler.spider, "kafka_topic", None)
        kafka_key_schema = getattr(
            crawler.spider, "kafka_key_schema", '{"type": "string"}'
        )
        kafka_value_schema = getattr(crawler.spider, "kafka_value_schema", None)

        return cls(kafka_topic, kafka_key_schema, kafka_value_schema)

    def process_item(self, item, spider):
        value = dict(item)

        # Produce the processed item into the kafka topic using Avro serialization
        key = str(uuid.uuid4())
        try:
            self.producer.produce(
                topic=self._kafka_topic,
                key_schema=self._kafka_key_schema,
                value_schema=self._kafka_value_schema,
                value=value,
                key=key,
            )
            self.producer.flush()
        except requests.exceptions.ConnectionError:
            self.logger.error(
                f"Cannot produce the item {key} to the {self._kafka_topic} topic."
            )

        return item
