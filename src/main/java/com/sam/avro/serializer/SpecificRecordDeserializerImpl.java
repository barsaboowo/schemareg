package com.sam.avro.serializer;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;

/**
 * Created by b on 10/7/18.
 */
@Slf4j
public class SpecificRecordDeserializerImpl implements SpecificRecordDeserializer {

    private final KafkaAvroDeserializer kafkaAvroDeserializer;

    public SpecificRecordDeserializerImpl(final String schemaRegistryUrl) {
        log.info("Schema Registry Url: {}", schemaRegistryUrl);
        this.kafkaAvroDeserializer = new KafkaAvroDeserializer(
                new CachedSchemaRegistryClient(schemaRegistryUrl, 1024),
                ImmutableMap.of(
                        "specific.avro.reader", true,
                        "schema.registry.url", schemaRegistryUrl
                )
        );
    }

    @Override
    public <R extends SpecificRecord> R deserialize(byte[] value, String schemaTopic) {
        return (R) kafkaAvroDeserializer.deserialize(schemaTopic, value);
    }

}
