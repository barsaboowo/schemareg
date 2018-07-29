package com.sam.avro.serializer;

import org.apache.avro.specific.SpecificRecord;

/**
 * Created by b on 10/7/18.
 */
public interface SpecificRecordSerializer {
    byte[] serialize(SpecificRecord value, String schemaTopic);
}
