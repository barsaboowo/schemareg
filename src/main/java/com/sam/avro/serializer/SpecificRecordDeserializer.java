package com.sam.avro.serializer;

import org.apache.avro.specific.SpecificRecord;

/**
 * Created by b on 10/7/18.
 */
public interface SpecificRecordDeserializer {
    <R extends SpecificRecord> R deserialize(byte[] value, String schemaTopic);
}
