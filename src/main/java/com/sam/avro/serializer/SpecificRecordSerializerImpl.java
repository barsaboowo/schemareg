package com.sam.avro.serializer;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

/**
 * Created by b on 10/7/18.
 */
@Slf4j
public class SpecificRecordSerializerImpl implements SpecificRecordSerializer {

    private final KafkaAvroSerializer serializer;

    public SpecificRecordSerializerImpl(final String schemaRegistryUrl) {
        log.info("Schema Registry Url: {}", schemaRegistryUrl);
        Map<String, ?> props = ImmutableMap.of(
                AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        );
        this.serializer = new KafkaAvroSerializer(new CachedSchemaRegistryClient(schemaRegistryUrl, 1024), props);
    }

    @Override
    public byte[] serialize(SpecificRecord value, String schemaTopic) {
        return serializer.serialize(schemaTopic, value);
    }
}
