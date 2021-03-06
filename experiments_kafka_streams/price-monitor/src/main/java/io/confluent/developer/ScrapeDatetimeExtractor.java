package io.confluent.developer;

import com.jasongoodwin.monads.Try;
import com.typesafe.config.Config;
import io.confluent.developer.avro.ProductItem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


public class ScrapeDatetimeExtractor implements TimestampExtractor {

    private DateTimeFormatter formatter;
    private static final Logger logger = LoggerFactory.getLogger(TimestampExtractor.class);

    public ScrapeDatetimeExtractor(Config config) {
        this.formatter = DateTimeFormatter.ofPattern(config.getString("scrape.date.datetime.pattern")).withZone(ZoneId.of("UTC"));;
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        logger.debug("extracting the scrape date from ", record);
        return Try

                .ofFailable(() -> ((ProductItem) record.value()).getScrapeDate())

                .onFailure((ex) -> logger.error("fail to cast the PressureAlert: ", ex))

                .map((stringDatetimeString) ->  ZonedDateTime.parse(stringDatetimeString, this.formatter))

                .onFailure((ex) -> logger.error("fail to parse the event datetime due to: ", ex))

                .map((zonedDatetime) -> zonedDatetime.toInstant().toEpochMilli())

                .onFailure((ex) -> logger.error("fail to convert the datetime to instant due to: ", ex))

                .orElse(-1L);
    }
}