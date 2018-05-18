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
package io.zeebe.client.impl.record;

import com.fasterxml.jackson.annotation.*;
import io.zeebe.client.api.record.RecordMetadata;
import io.zeebe.protocol.clientapi.ExecuteCommandRequestEncoder;
import io.zeebe.protocol.intent.Intent;

public class RecordMetadataImpl implements RecordMetadata
{
    private String topicName;
    private int partitionId = ExecuteCommandRequestEncoder.partitionIdNullValue();
    private long key = ExecuteCommandRequestEncoder.keyNullValue();
    private long position = ExecuteCommandRequestEncoder.positionNullValue();
    private io.zeebe.protocol.clientapi.RecordType recordType;
    private io.zeebe.protocol.clientapi.ValueType valueType;
    private Intent intent;

    public RecordMetadataImpl()
    {
        // default constructor
    }

    @JsonCreator
    public RecordMetadataImpl(
            @JsonProperty("intent") String intent,
            @JsonProperty("valueType") io.zeebe.protocol.clientapi.ValueType valueType,
            @JsonProperty("recordType") io.zeebe.protocol.clientapi.RecordType recordType)
    {
        // is used by Jackson to de-serialize a JSON String
        // resolve the intent from the given String and the value type
        this.valueType = valueType;
        this.recordType = recordType;
        this.intent = Intent.fromProtocolValue(valueType, intent);
    }

    @Override
    public String getTopicName()
    {
        return topicName;
    }

    public void setTopicName(String topicName)
    {
        this.topicName = topicName;
    }

    @Override
    public int getPartitionId()
    {
        return partitionId;
    }

    public void setPartitionId(int partitionId)
    {
        this.partitionId = partitionId;
    }

    public boolean hasPartitionId()
    {
        return partitionId != ExecuteCommandRequestEncoder.partitionIdNullValue();
    }

    @Override
    public long getPosition()
    {
        return position;
    }

    public void setPosition(long position)
    {
        this.position = position;
    }

    @Override
    public long getKey()
    {
        return key;
    }

    public void setKey(long key)
    {
        this.key = key;
    }

    @Override
    public RecordType getRecordType()
    {
        return RecordType.valueOf(recordType.name());
    }

    @JsonIgnore
    public io.zeebe.protocol.clientapi.RecordType getProtocolRecordType()
    {
        return recordType;
    }

    public void setRecordType(io.zeebe.protocol.clientapi.RecordType recordType)
    {
        this.recordType = recordType;
    }

    @Override
    public ValueType getValueType()
    {
        return ValueType.valueOf(valueType.name());
    }

    @JsonIgnore
    public io.zeebe.protocol.clientapi.ValueType getProtocolValueType()
    {
        return valueType;
    }

    public void setValueType(io.zeebe.protocol.clientapi.ValueType valueType)
    {
        this.valueType = valueType;
    }

    @Override
    public String getIntent()
    {
        return intent.name();
    }

    @JsonIgnore
    public Intent getProtocolIntent()
    {
        return intent;
    }

    public void setIntent(Intent intent)
    {
        this.intent = intent;
    }

    @Override
    public String toString()
    {
        return "EventMetadata [topicName=" + topicName + ", partitionId=" + partitionId + ", key=" +
                key + ", position=" + position + ", eventType=" + recordType + "]";
    }

}
