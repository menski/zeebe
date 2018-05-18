/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.client.impl;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.zeebe.client.api.events.JobEvent;
import io.zeebe.client.api.record.Record;
import io.zeebe.client.api.record.ZeebeObjectMapper;
import io.zeebe.client.impl.data.MsgPackConverter;
import io.zeebe.client.impl.data.PayloadField;
import io.zeebe.client.impl.event.JobEventImpl;
import io.zeebe.client.impl.record.*;
import io.zeebe.protocol.Protocol;
import org.msgpack.jackson.dataformat.MessagePackFactory;

public class ZeebeObjectMapperImpl implements ZeebeObjectMapper
{
    private final ObjectMapper msgpackObjectMapper;
    private final ObjectMapper jsonObjectMapper;
    private final InjectableValues.Std injectableValues;

    private static final Map<Class<?>, Class<?>> RECORD_IMPL_CLASS_MAPPING;

    static
    {
        RECORD_IMPL_CLASS_MAPPING = new HashMap<>();
        RECORD_IMPL_CLASS_MAPPING.put(JobEvent.class, JobEventImpl.class);
    }

    abstract class MsgpackRecordMixin
    {
        // records from broker does't have metadata inside (instead it's part of SBE layer)
        @JsonIgnore
        abstract RecordMetadataImpl getMetadata();
    }

    class MsgpackInstantSerializer extends StdSerializer<Instant>
    {

        protected MsgpackInstantSerializer()
        {
            this(null);
        }

        protected MsgpackInstantSerializer(Class<Instant> t)
        {
            super(t);
        }

        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider provider) throws IOException
        {
            if (value == null)
            {
                gen.writeNumber(Protocol.INSTANT_NULL_VALUE);
            }
            else
            {
                final long epochMilli = value.toEpochMilli();
                gen.writeNumber(epochMilli);
            }
        }

    }

    class MsgpackInstantDeserializer extends StdDeserializer<Instant>
    {
        protected MsgpackInstantDeserializer()
        {
            this(null);
        }

        protected MsgpackInstantDeserializer(Class<?> vc)
        {
            super(vc);
        }

        @Override
        public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            final long epochMilli = p.getLongValue();

            if (epochMilli == Protocol.INSTANT_NULL_VALUE)
            {
                return null;
            }
            else
            {
                return Instant.ofEpochMilli(epochMilli);
            }

        }

    }

    class MsgpackPayloadSerializer extends StdSerializer<PayloadField>
    {

        protected MsgpackPayloadSerializer()
        {
            this(null);
        }

        protected MsgpackPayloadSerializer(Class<PayloadField> t)
        {
            super(t);
        }

        @Override
        public void serialize(PayloadField value, JsonGenerator gen, SerializerProvider provider) throws IOException
        {
            gen.writeBinary(value.getMsgPack());
        }

    }

    class MsgpackPayloadDeserializer extends StdDeserializer<PayloadField>
    {
        private MsgPackConverter msgPackConverter;
        private ZeebeObjectMapperImpl objectMapper;

        protected MsgpackPayloadDeserializer(ZeebeObjectMapperImpl objectMapper, MsgPackConverter msgPackConverter)
        {
            this(null);
            this.msgPackConverter = msgPackConverter;
            this.objectMapper = objectMapper;
        }

        protected MsgpackPayloadDeserializer(Class<?> vc)
        {
            super(vc);
        }

        @Override
        public PayloadField deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            final byte[] msgpackPayload = p.getBinaryValue();

            final PayloadField payload = new PayloadField(objectMapper, msgPackConverter);
            payload.setMsgPack(msgpackPayload);

            return payload;
        }

    }

    class StringPayloadSerializer extends StdSerializer<PayloadField>
    {

        protected StringPayloadSerializer()
        {
            this(null);
        }

        protected StringPayloadSerializer(Class<PayloadField> t)
        {
            super(t);
        }

        @Override
        public void serialize(PayloadField value, JsonGenerator gen, SerializerProvider provider) throws IOException
        {
            final String json = value.getAsJsonString();
            gen.writeRawValue(json);
        }

    }

    class StringPayloadDeserializer extends StdDeserializer<PayloadField>
    {
        private MsgPackConverter msgPackConverter;
        private ZeebeObjectMapperImpl objectMapper;

        protected StringPayloadDeserializer(ZeebeObjectMapperImpl objectMapper, MsgPackConverter msgPackConverter)
        {
            this(null);
            this.msgPackConverter = msgPackConverter;
            this.objectMapper = objectMapper;
        }

        protected StringPayloadDeserializer(Class<?> vc)
        {
            super(vc);
        }

        @Override
        public PayloadField deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            final TreeNode node = p.readValueAsTree();
            final String json = node.toString();
            // objectMapper.toJson(node)

            final PayloadField payload = new PayloadField(objectMapper, msgPackConverter);
            payload.setJson(json);

            return payload;
        }

    }

    public ZeebeObjectMapperImpl(MsgPackConverter msgPackConverter)
    {
        msgpackObjectMapper = new ObjectMapper(new MessagePackFactory().setReuseResourceInGenerator(false).setReuseResourceInParser(false));

        msgpackObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        msgpackObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        msgpackObjectMapper.addMixIn(RecordImpl.class, MsgpackRecordMixin.class);

        final SimpleModule msgpackModule = new SimpleModule();
        msgpackModule.addSerializer(Instant.class, new MsgpackInstantSerializer());
        msgpackModule.addDeserializer(Instant.class, new MsgpackInstantDeserializer());

        msgpackModule.addSerializer(PayloadField.class, new MsgpackPayloadSerializer());
        msgpackModule.addDeserializer(PayloadField.class, new MsgpackPayloadDeserializer(this, msgPackConverter));

        msgpackObjectMapper.registerModule(msgpackModule);

        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // serialize INSTANT as ISO-8601 String
        jsonObjectMapper.registerModule(new JavaTimeModule());
        jsonObjectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        final SimpleModule stringPayloadModule = new SimpleModule();
        stringPayloadModule.addSerializer(PayloadField.class, new StringPayloadSerializer());
        stringPayloadModule.addDeserializer(PayloadField.class, new StringPayloadDeserializer(this, msgPackConverter));
        jsonObjectMapper.registerModule(stringPayloadModule);


        this.injectableValues = new InjectableValues.Std();

        injectableValues.addValue(MsgPackConverter.class, msgPackConverter);
        injectableValues.addValue(ZeebeObjectMapper.class, this);
        injectableValues.addValue(ZeebeObjectMapperImpl.class, this);

        msgpackObjectMapper.setInjectableValues(injectableValues);
        jsonObjectMapper.setInjectableValues(injectableValues);
    }

    @Override
    public String toJson(Record record)
    {
        try
        {
            return jsonObjectMapper.writeValueAsString(record);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(String.format("Failed to serialize object '%s' to JSON", record), e);
        }
    }

    @Override
    public <T extends Record> T fromJson(String json, Class<T> recordClass)
    {
        @SuppressWarnings("unchecked")
        final Class<T> recordImplClass = (Class<T>) RECORD_IMPL_CLASS_MAPPING.get(recordClass);
        if (recordImplClass == null)
        {
            throw new RuntimeException(String.format("Cannot deserialize JSON: unknown record class '%s'", recordClass));
        }

        try
        {
            return jsonObjectMapper.readValue(json, recordImplClass);
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed deserialize JSON '%s' to object of type '%s'", json, recordClass), e);
        }
    }

    public <T extends Record> T fromJson(byte[] json, Class<T> recordClass)
    {
        try
        {
            return msgpackObjectMapper.readValue(json, recordClass);
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed deserialize JSON byte array to object of type '%s'", recordClass), e);
        }
    }

    public void toJson(OutputStream outputStream, Object value)
    {
        try
        {
            msgpackObjectMapper.writeValue(outputStream, value);
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed to serialize object '%s' to JSON", value), e);
        }
    }

    public <T> T fromJson(InputStream inputStream, Class<T> valueType)
    {
        try
        {
            return msgpackObjectMapper.readValue(inputStream, valueType);
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed deserialize JSON stream to object of type '%s'", valueType), e);
        }
    }

    public Object fromJson(String json)
    {
        try
        {
            return jsonObjectMapper.readValue(json, Map.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed to deserialize JSON '%s'", json), e);
        }
    }

    public String toJson(Object json)
    {
        try
        {
            return jsonObjectMapper.writeValueAsString(json);
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed to serialize object to JSON '%s'", json), e);
        }
    }

}
