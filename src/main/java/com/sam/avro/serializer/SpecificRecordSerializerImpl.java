package com.sam.avro.serializer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;

/**
 * Created by b on 10/7/18.
 */
@Slf4j
public class SpecificRecordSerializerImpl implements SpecificRecordSerializer {

    private final KafkaAvroSerializer serializer;

    public SpecificRecordSerializerImpl(final String schemaRegistryUrl) {
        log.info("Schema Registry Url: {}", schemaRegistryUrl);
        this.serializer = new KafkaAvroSerializer(new CachedSchemaRegistryClient(schemaRegistryUrl, 1024));
    }

    @Override
    public byte[] serialize(SpecificRecord value, String schemaTopic) {
        return serializer.serialize(schemaTopic, value);
    }
}
