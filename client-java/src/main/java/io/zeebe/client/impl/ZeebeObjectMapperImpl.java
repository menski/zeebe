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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import io.zeebe.client.api.events.JobEvent;
import io.zeebe.client.api.record.Record;
import io.zeebe.client.api.record.ZeebeObjectMapper;
import io.zeebe.client.impl.data.MsgPackConverter;
import io.zeebe.client.impl.event.JobEventImpl;
import io.zeebe.client.impl.record.JobRecordImpl;
import io.zeebe.client.impl.record.RecordMetadataImpl;
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

    // don't use this for other thing - this is MAGIC!!!
    abstract class StringPayloadMixin
    {
        @JsonIgnore
        abstract byte[] getPayloadMsgPack();

        @JsonIgnore
        abstract void setPayload(byte[] msgPack);
    }

    abstract class MsgpackPayloadMixin
    {
        @JsonIgnore
        abstract String getPayload();

        @JsonIgnore
        abstract void setPayloadObject(JsonNode payload);

        @JsonIgnore
        abstract RecordMetadataImpl getMetadata();
    }

    abstract class JobRecordMsgpackMixin extends MsgpackPayloadMixin
    {
        @JsonIgnore
        abstract String getLockTimeAsString();

        @JsonIgnore
        abstract void setLockTime(String lockTime);
    }

    abstract class JobRecordJsonMixin extends StringPayloadMixin
    {
        @JsonIgnore
        abstract Instant getLockTime();

        @JsonIgnore
        abstract void setLockTime(long lockTime);
    }

    public ZeebeObjectMapperImpl(MsgPackConverter msgPackConverter)
    {
        msgpackObjectMapper = new ObjectMapper(new MessagePackFactory().setReuseResourceInGenerator(false).setReuseResourceInParser(false));

        msgpackObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        msgpackObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        //msgpackObjectMapper.addMixIn(JobRecordImpl.class, MsgpackPayloadMixin.class);
        msgpackObjectMapper.addMixIn(JobRecordImpl.class, JobRecordMsgpackMixin.class);

        jsonObjectMapper = new ObjectMapper();
        //jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        //jsonObjectMapper.addMixIn(JobRecordImpl.class, StringPayloadMixin.class);
        jsonObjectMapper.addMixIn(JobRecordImpl.class, JobRecordJsonMixin.class);

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
            // TODO Use {@link #writeValueAsBytes(Object)} instead
            // return objectMapper.writeValueAsString(record);
            //return new String(objectMapper.writeValueAsBytes(record));
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
