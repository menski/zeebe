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
package io.zeebe.client.impl.data;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.zeebe.client.api.commands.*;
import io.zeebe.client.api.events.*;
import io.zeebe.client.api.record.Record;
import io.zeebe.client.api.record.ZeebeObjectMapper;
import io.zeebe.client.impl.command.*;
import io.zeebe.client.impl.event.*;
import io.zeebe.client.impl.record.RecordImpl;
import io.zeebe.client.impl.record.RecordMetadataImpl;
import org.msgpack.jackson.dataformat.MessagePackFactory;

public class ZeebeObjectMapperImpl implements ZeebeObjectMapper
{
    private final ObjectMapper msgpackObjectMapper;
    private final ObjectMapper defaultObjectMapper;
    private final InjectableValues.Std injectableValues;

    private static final Map<Class<?>, Class<?>> RECORD_IMPL_CLASS_MAPPING;

    static
    {
        RECORD_IMPL_CLASS_MAPPING = new HashMap<>();
        RECORD_IMPL_CLASS_MAPPING.put(JobEvent.class, JobEventImpl.class);
        RECORD_IMPL_CLASS_MAPPING.put(JobCommand.class, JobCommandImpl.class);
        RECORD_IMPL_CLASS_MAPPING.put(WorkflowInstanceEvent.class, WorkflowInstanceEventImpl.class);
        RECORD_IMPL_CLASS_MAPPING.put(WorkflowInstanceCommand.class, WorkflowInstanceCommandImpl.class);
        RECORD_IMPL_CLASS_MAPPING.put(DeploymentEvent.class, DeploymentEventImpl.class);
        RECORD_IMPL_CLASS_MAPPING.put(DeploymentCommand.class, DeploymentCommandImpl.class);
        RECORD_IMPL_CLASS_MAPPING.put(IncidentEvent.class, IncidentEventImpl.class);
        RECORD_IMPL_CLASS_MAPPING.put(IncidentCommand.class, IncidentCommandImpl.class);
        RECORD_IMPL_CLASS_MAPPING.put(RaftEvent.class, RaftEventImpl.class);
    }

    public ZeebeObjectMapperImpl(MsgPackConverter msgPackConverter)
    {
        msgpackObjectMapper = new ObjectMapper(new MessagePackFactory().setReuseResourceInGenerator(false).setReuseResourceInParser(false));

        msgpackObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        msgpackObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        msgpackObjectMapper.addMixIn(RecordImpl.class, MsgpackRecordMixin.class);

        msgpackObjectMapper.registerModule(new MsgpackIntentModule());
        msgpackObjectMapper.registerModule(new MsgpackPayloadModule(msgPackConverter));

        defaultObjectMapper = new ObjectMapper();
        defaultObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        defaultObjectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        defaultObjectMapper.registerModule(new JavaTimeModule()); // for serialize INSTANT
        defaultObjectMapper.registerModule(new PayloadModule(msgPackConverter));

        injectableValues = new InjectableValues.Std();
        injectableValues.addValue(MsgPackConverter.class, msgPackConverter);
        injectableValues.addValue(ZeebeObjectMapper.class, this);
        injectableValues.addValue(ZeebeObjectMapperImpl.class, this);

        msgpackObjectMapper.setInjectableValues(injectableValues);
        defaultObjectMapper.setInjectableValues(injectableValues);
    }

    @Override
    public String toJson(Record record)
    {
        try
        {
            return defaultObjectMapper.writeValueAsString(record);
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
        final Class<T> implClass = (Class<T>) RECORD_IMPL_CLASS_MAPPING.get(recordClass);
        if (implClass == null)
        {
            throw new RuntimeException(String.format("Cannot deserialize JSON: unknown record class '%s'", recordClass));
        }

        try
        {
            return defaultObjectMapper.readValue(json, implClass);
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
            return defaultObjectMapper.readValue(json, Map.class);
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
            return defaultObjectMapper.writeValueAsString(json);
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed to serialize object to JSON '%s'", json), e);
        }
    }

    abstract class MsgpackRecordMixin
    {
        // records from broker does't have metadata inside (instead it's part of SBE layer)
        @JsonIgnore
        abstract RecordMetadataImpl getMetadata();
    }

}
