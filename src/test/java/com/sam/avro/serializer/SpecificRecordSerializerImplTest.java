package com.sam.avro.serializer;

import com.sam.avro.dto.PayloadDto;
import com.sam.avro.dto.UserDto;
import junit.framework.TestCase;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by b on 29/7/18.
 */
public class SpecificRecordSerializerImplTest extends TestCase {

    private static final String schemaRegistryUrl = "http://localhost:8080";

    private SpecificRecordSerializer specificRecordSerializer;
    private SpecificRecordDeserializer specificRecordDeserializer;
    private PayloadDtoFactory payloadDtoFactory;
    public static final UserDto USER_DTO = UserDto.newBuilder().setEmail("samueledwin.barber@sc.com")
            .setFirstName("Sam")
            .setLastName("Barber")
            .setUserId(UUID.randomUUID().toString()).build();

    public void setUp() throws Exception {
        super.setUp();

        this.specificRecordSerializer = new SpecificRecordSerializerImpl(schemaRegistryUrl);
        this.specificRecordDeserializer = new SpecificRecordDeserializerImpl(schemaRegistryUrl);

        payloadDtoFactory = new PayloadDtoFactoryImpl(specificRecordSerializer, specificRecordDeserializer);

    }

    @Test
    public void testSerialize() throws Exception {

        byte[] serialize = specificRecordSerializer.serialize(USER_DTO, UserDto.class.getCanonicalName());

        PayloadDto flowEventId1 = PayloadDto.newBuilder().setFlowEventId("flowEventId")
                .setPayload(ByteBuffer.wrap(serialize)).build();


        byte[] serialize1 = specificRecordSerializer.serialize(flowEventId1, PayloadDto.class.getCanonicalName());


        PayloadDto deserializedPayload = specificRecordDeserializer.deserialize(serialize1, PayloadDto.class.getCanonicalName());

        assertEquals(flowEventId1, deserializedPayload);

        UserDto deserializedUser = specificRecordDeserializer.deserialize(deserializedPayload.getPayload().array(), UserDto.class.getCanonicalName());

        assertEquals(USER_DTO, deserializedUser);


    }

    @Test
    public void testFactory() throws Exception {
        PayloadDto payloadDto = payloadDtoFactory.newInstance(USER_DTO, "payload1");

        assertEquals(USER_DTO, payloadDtoFactory.extractRecord(payloadDto));

    }
}