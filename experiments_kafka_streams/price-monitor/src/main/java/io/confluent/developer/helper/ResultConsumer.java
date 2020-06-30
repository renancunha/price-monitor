package io.confluent.developer.helper;


import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.scaladsl.Sink;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.developer.avro.AggSkuData;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.streams.kstream.WindowedSerdes.timeWindowedSerdeFrom;

public class ResultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ResultConsumer.class);

    public static void main(String[] args) {

        final Config config = ConfigFactory.load();
        final String outputTopic = config.getString("output.topic.name");

        final ActorSystem system = ActorSystem.create();
        final Materializer materializer = ActorMaterializer.create(system);

        Map<String, Object> serdeConfig =
                singletonMap(SCHEMA_REGISTRY_URL_CONFIG, config.getString("schema.registry.url"));

        SpecificAvroSerde<AggSkuData> aggSkuDataSerde = new SpecificAvroSerde<>();
        aggSkuDataSerde.configure(serdeConfig, false);

        final ConsumerSettings<Windowed<String>, AggSkuData> consumerSettings =
                ConsumerSettings
                        .create(
                                system,
                                timeWindowedSerdeFrom(
                                        String.class,
                                        config.getDuration("window.size").toMillis()
                                ).deserializer(),
                                aggSkuDataSerde.deserializer()
                        )
                        .withGroupId(UUID.randomUUID().toString())
                        .withBootstrapServers(config.getString("bootstrap.servers"))
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer.plainSource(
                consumerSettings,
                Subscriptions.topics(outputTopic))
                .to(Sink.foreach((record) -> {
                            logger.info(printWindowedKey(config, record));
                            return BoxedUnit.UNIT;
                        })
                ).run(materializer);

    }

    private static String printWindowedKey(Config config, ConsumerRecord<Windowed<String>, AggSkuData> windowedKeyValue) {
        String store = windowedKeyValue.value().getUrl().contains("walmart") ? "walmart" : "bestbuy";
        return String.format("Min = %s for Key = %s, at window [%s-%s] on %s",
                windowedKeyValue.value().getMin(),
                windowedKeyValue.key().key(),
                DateTimeFormatter
                        .ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withLocale(Locale.getDefault())
                        .withZone(ZoneId.systemDefault())
                        .format(windowedKeyValue.key().window().startTime()),
                DateTimeFormatter
                        .ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withLocale(Locale.getDefault())
                        .withZone(ZoneId.systemDefault())
                        .format(windowedKeyValue.key().window().endTime()),
                store
        );
    }
}