package com.sam.avro.serializer;

import com.sam.avro.dto.PayloadDto;
import org.apache.avro.specific.SpecificRecord;

import java.nio.ByteBuffer;

/**
 * Created by b on 29/7/18.
 */
public class PayloadDtoFactoryImpl implements PayloadDtoFactory {

    private final SpecificRecordSerializer specificRecordSerializer;
    private final SpecificRecordDeserializer specificRecordDeserializer;

    public PayloadDtoFactoryImpl(SpecificRecordSerializer specificRecordSerializer, SpecificRecordDeserializer specificRecordDeserializer) {
        this.specificRecordSerializer = specificRecordSerializer;
        this.specificRecordDeserializer = specificRecordDeserializer;
    }

    @Override
    public PayloadDto newInstance(SpecificRecord specificRecord, String payloadId) {
        String canonicalName = specificRecord.getClass().getCanonicalName();
        byte[] serialize = specificRecordSerializer.serialize(specificRecord, canonicalName);
        return PayloadDto.newBuilder()
                .setPayload(ByteBuffer.wrap(serialize))
                .setSchemaRegistryTopic(canonicalName)
                .setFlowEventId(payloadId)
                .build();
    }

    @Override
    public <V extends SpecificRecord> V extractRecord(PayloadDto payloadDto) {
        return specificRecordDeserializer.deserialize(payloadDto.getPayload().array(), payloadDto.getSchemaRegistryTopic().toString());
    }
}
