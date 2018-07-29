package com.sam.avro.serializer;

import com.sam.avro.dto.PayloadDto;
import org.apache.avro.specific.SpecificRecord;

/**
 * Created by b on 29/7/18.
 */
public interface PayloadDtoFactory {
    PayloadDto newInstance(SpecificRecord specificRecord, String payloadId);

    <V extends SpecificRecord> V extractRecord(PayloadDto payloadDto);
}
