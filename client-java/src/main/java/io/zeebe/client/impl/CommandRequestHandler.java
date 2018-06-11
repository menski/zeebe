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

import io.zeebe.client.api.record.RecordMetadata;
import io.zeebe.client.cmd.ClientCommandRejectedException;
import io.zeebe.client.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.record.RecordImpl;
import io.zeebe.client.impl.record.RecordMetadataImpl;
import io.zeebe.protocol.clientapi.ExecuteCommandRequestEncoder;
import io.zeebe.protocol.clientapi.ExecuteCommandResponseDecoder;
import io.zeebe.protocol.clientapi.MessageHeaderDecoder;
import io.zeebe.protocol.clientapi.MessageHeaderEncoder;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.Intent;
import java.time.Instant;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.agrona.io.ExpandableDirectBufferOutputStream;

public class CommandRequestHandler implements RequestResponseHandler {
  protected MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  protected ExecuteCommandRequestEncoder encoder = new ExecuteCommandRequestEncoder();

  protected ExecuteCommandResponseDecoder decoder = new ExecuteCommandResponseDecoder();

  protected RecordImpl command;

  protected final ZeebeObjectMapperImpl objectMapper;

  protected ExpandableArrayBuffer serializedCommand = new ExpandableArrayBuffer();
  protected int serializedCommandLength = 0;

  @SuppressWarnings("rawtypes")
  public CommandRequestHandler(ZeebeObjectMapperImpl objectMapper, CommandImpl command) {
    this.objectMapper = objectMapper;
    this.command = command.getCommand();
    serialize(command.getCommand());
  }

  protected void serialize(RecordImpl event) {
    int offset = 0;
    headerEncoder
        .wrap(serializedCommand, offset)
        .blockLength(encoder.sbeBlockLength())
        .schemaId(encoder.sbeSchemaId())
        .templateId(encoder.sbeTemplateId())
        .version(encoder.sbeSchemaVersion());

    offset += headerEncoder.encodedLength();

    encoder.wrap(serializedCommand, offset);

    final RecordMetadataImpl metadata = event.getMetadata();

    encoder.partitionId(metadata.getPartitionId()).position(metadata.getPosition());

    if (metadata.getKey() < 0) {
      encoder.key(ExecuteCommandRequestEncoder.keyNullValue());
    } else {
      encoder.key(metadata.getKey());
    }

    encoder.valueType(metadata.getProtocolValueType());
    encoder.intent(metadata.getProtocolIntent().value());

    offset = encoder.limit();
    final int commandHeaderOffset = offset;
    final int serializedCommandOffset =
        commandHeaderOffset + ExecuteCommandRequestEncoder.valueHeaderLength();

    final ExpandableDirectBufferOutputStream out =
        new ExpandableDirectBufferOutputStream(serializedCommand, serializedCommandOffset);

    objectMapper.toMsgpack(out, event);

    // can only write the header after we have written the command, as we don't know the length
    // beforehand
    final short commandLength = (short) out.position();
    serializedCommand.putShort(
        commandHeaderOffset, commandLength, java.nio.ByteOrder.LITTLE_ENDIAN);

    serializedCommandLength = serializedCommandOffset + out.position();
  }

  @Override
  public int getLength() {
    return serializedCommandLength;
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    buffer.putBytes(offset, serializedCommand, 0, serializedCommandLength);
  }

  @Override
  public boolean handlesResponse(MessageHeaderDecoder responseHeader) {
    return responseHeader.schemaId() == ExecuteCommandResponseDecoder.SCHEMA_ID
        && responseHeader.templateId() == ExecuteCommandResponseDecoder.TEMPLATE_ID;
  }

  @Override
  public RecordImpl getResult(DirectBuffer buffer, int offset, int blockLength, int version) {
    decoder.wrap(buffer, offset, blockLength, version);

    final int partitionId = decoder.partitionId();
    final long position = decoder.position();
    final long key = decoder.key();
    final RecordType recordType = decoder.recordType();
    final Instant timestamp = Instant.ofEpochMilli(decoder.timestamp());

    final ValueType valueType = decoder.valueType();
    final Intent intent = Intent.fromProtocolValue(valueType, decoder.intent());
    final RejectionType rejectionType = decoder.rejectionType();

    final int valueLength = decoder.valueLength();

    final DirectBufferInputStream inStream =
        new DirectBufferInputStream(
            buffer,
            decoder.limit() + ExecuteCommandResponseDecoder.valueHeaderLength(),
            valueLength);

    final Class<? extends RecordImpl> resultClass =
        recordType == RecordType.EVENT ? command.getEventClass() : command.getClass();

    final RecordImpl result = objectMapper.fromMsgpack(inStream, resultClass);

    decoder.limit(
        offset
            + ExecuteCommandResponseDecoder.BLOCK_LENGTH
            + ExecuteCommandResponseDecoder.valueHeaderLength()
            + valueLength);
    final String rejectionReason = decoder.rejectionReason();

    if (recordType == RecordType.COMMAND_REJECTION) {
      final String exceptionMessage =
          buildRejectionMessage(command, rejectionType, rejectionReason);
      throw new ClientCommandRejectedException(exceptionMessage);
    }

    final RecordMetadataImpl metadata = result.getMetadata();
    metadata.setKey(key);
    metadata.setPartitionId(partitionId);
    metadata.setTopicName(command.getMetadata().getTopicName());
    metadata.setPosition(position);
    metadata.setRecordType(recordType);
    metadata.setValueType(valueType);
    metadata.setIntent(intent);
    metadata.setTimestamp(timestamp);

    return result;
  }

  private String buildRejectionMessage(
      RecordImpl command, RejectionType rejectionType, String rejectionReason) {
    final long key = command.getMetadata().getKey();
    final StringBuilder sb = new StringBuilder();
    sb.append("Command (");
    sb.append(command.getMetadata().getIntent());
    sb.append(") ");

    if (key >= 0) {
      sb.append("for event with key ");
      sb.append(key);
      sb.append(" ");
    }

    sb.append("was rejected. ");
    sb.append(describeRejectionType(rejectionType));
    sb.append(" ");
    sb.append(rejectionReason);

    return sb.toString();
  }

  private String describeRejectionType(RejectionType rejectionType) {
    switch (rejectionType) {
      case BAD_VALUE:
        return "It has an invalid value.";
      case NOT_APPLICABLE:
        return "It is not applicable in the current state.";
      case PROCESSING_ERROR:
        return "The broker could not process it for internal reasons.";
      default:
        // Nothing
        return "";
    }
  }

  @Override
  public String getTargetTopic() {
    final RecordMetadata metadata = command.getMetadata();
    return metadata.getTopicName();
  }

  @Override
  public int getTargetPartition() {
    if (command.hasValidPartitionId()) {
      return command.getMetadata().getPartitionId();
    } else {
      return -1;
    }
  }

  @Override
  public void onSelectedPartition(int partitionId) {
    command.setPartitionId(partitionId);
    encoder.partitionId(partitionId);
  }

  @Override
  public String describeRequest() {
    final RecordMetadata metadata = command.getMetadata();
    return "[ topic = "
        + metadata.getTopicName()
        + ", partition = "
        + (command.hasValidPartitionId() ? metadata.getPartitionId() : "any")
        + ", value type = "
        + metadata.getValueType()
        + ", command = "
        + metadata.getIntent()
        + " ]";
  }
}
