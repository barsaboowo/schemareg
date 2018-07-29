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

    public void setUp() throws Exception {
        super.setUp();

        this.specificRecordSerializer = new SpecificRecordSerializerImpl(schemaRegistryUrl);
        this.specificRecordDeserializer = new SpecificRecordDeserializerImpl(schemaRegistryUrl);

    }

    @Test
    public void testSerialize() throws Exception {
        UserDto userDto = UserDto.newBuilder().setEmail("samueledwin.barber@sc.com")
                .setFirstName("Sam")
                .setLastName("Barber")
                .setUserId(UUID.randomUUID().toString()).build();

        byte[] serialize = specificRecordSerializer.serialize(userDto, UserDto.class.getCanonicalName());

        PayloadDto flowEventId1 = PayloadDto.newBuilder().setFlowEventId("flowEventId")
                .setPayload(ByteBuffer.wrap(serialize)).build();


        byte[] serialize1 = specificRecordSerializer.serialize(flowEventId1, PayloadDto.class.getCanonicalName());


        PayloadDto deserializedPayload = specificRecordDeserializer.deserialize(serialize1, PayloadDto.class.getCanonicalName());

        assertEquals(flowEventId1, deserializedPayload);

        UserDto deserializedUser = specificRecordDeserializer.deserialize(deserializedPayload.getPayload().array(), UserDto.class.getCanonicalName());

        assertEquals(userDto, deserializedUser);


    }

}