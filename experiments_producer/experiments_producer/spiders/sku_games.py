# -*- coding: utf-8 -*-
import pkg_resources
import scrapy
import json

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst
from scrapy_jsonschema.item import JsonSchemaItem

json_schema = pkg_resources.resource_string(
    "experiments_producer", "spiders/res/sku_schema.json"
).decode("utf8")

avro_value_schema = pkg_resources.resource_string(
    "experiments_producer", "spiders/res/sku_schema.avsc"
).decode("utf8")


class SkuGamesItem(JsonSchemaItem):
    jsonschema = json.loads(json_schema)


class SkuGamesItemLoader(ItemLoader):
    default_item_class = SkuGamesItem
    default_output_processor = TakeFirst()


class SkuGamesSpider(scrapy.Spider):
    name = "sku_games"
    start_urls = ["http://www.google.com"]

    # Kafka settings
    kafka_export_enabled = True
    kafka_topic = "input-topic"
    kafka_value_schema = avro_value_schema

    def parse(self, response):
        data_path = pkg_resources.resource_string(
            "experiments_producer", "spiders/res/sku_data.json"
        ).decode("utf8")
        data = json.loads(data_path)

        for result in data:
            il = SkuGamesItemLoader(result.copy())
            for k, v in result.items():
                il.add_value(k, v)

            item = il.load_item()
            yield item

