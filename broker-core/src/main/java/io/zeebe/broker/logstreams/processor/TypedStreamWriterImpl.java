/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.logstreams.processor;

import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamBatchWriter;
import io.zeebe.logstreams.log.LogStreamBatchWriter.LogEntryBuilder;
import io.zeebe.logstreams.log.LogStreamBatchWriterImpl;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.Intent;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class TypedStreamWriterImpl implements TypedStreamWriter, TypedBatchWriter {
  protected final Consumer<RecordMetadata> noop = m -> {};

  protected RecordMetadata metadata = new RecordMetadata();
  protected final Map<Class<? extends UnpackedObject>, ValueType> typeRegistry;
  protected final LogStream stream;

  protected LogStreamWriter writer;
  protected LogStreamBatchWriter batchWriter;

  protected int producerId;
  protected long sourceRecordPosition;

  public TypedStreamWriterImpl(
      LogStream stream, Map<ValueType, Class<? extends UnpackedObject>> eventRegistry) {
    this.stream = stream;
    metadata.protocolVersion(Protocol.PROTOCOL_VERSION);
    this.writer = new LogStreamWriterImpl(stream);
    this.batchWriter = new LogStreamBatchWriterImpl(stream);
    this.typeRegistry = new HashMap<>();
    eventRegistry.forEach((e, c) -> typeRegistry.put(c, e));
  }

  public void configureSourceContext(int producerId, long sourceRecordPosition) {
    this.producerId = producerId;
    this.sourceRecordPosition = sourceRecordPosition;
  }

  protected void initMetadata(RecordType type, Intent intent, UnpackedObject value) {
    metadata.reset();
    final ValueType valueType = typeRegistry.get(value.getClass());

    metadata.recordType(type);
    metadata.valueType(valueType);
    metadata.intent(intent);
  }

  private long writeRecord(
      long key,
      RecordType type,
      Intent intent,
      UnpackedObject value,
      Consumer<RecordMetadata> additionalMetadata) {
    return writeRecord(key, type, intent, RejectionType.NULL_VAL, "", value, additionalMetadata);
  }

  private long writeRecord(
      long key,
      RecordType type,
      Intent intent,
      RejectionType rejectionType,
      String rejectionReason,
      UnpackedObject value,
      Consumer<RecordMetadata> additionalMetadata) {
    writer.reset();
    writer.producerId(producerId);

    if (sourceRecordPosition >= 0) {
      writer.sourceRecordPosition(sourceRecordPosition);
    }

    initMetadata(type, intent, value);
    metadata.rejectionType(rejectionType);
    metadata.rejectionReason(rejectionReason);
    additionalMetadata.accept(metadata);

    if (key >= 0) {
      writer.key(key);
    } else {
      writer.positionAsKey();
    }

    return writer.metadataWriter(metadata).valueWriter(value).tryWrite();
  }

  @Override
  public long writeNewCommand(Intent intent, UnpackedObject value) {
    return writeRecord(-1, RecordType.COMMAND, intent, value, noop);
  }

  @Override
  public long writeFollowUpCommand(long key, Intent intent, UnpackedObject value) {
    return writeRecord(key, RecordType.COMMAND, intent, value, noop);
  }

  @Override
  public long writeFollowUpCommand(
      long key, Intent intent, UnpackedObject value, Consumer<RecordMetadata> metadata) {
    return writeRecord(key, RecordType.COMMAND, intent, value, metadata);
  }

  @Override
  public long writeNewEvent(Intent intent, UnpackedObject value) {
    return writeRecord(-1, RecordType.EVENT, intent, value, noop);
  }

  @Override
  public long writeFollowUpEvent(long key, Intent intent, UnpackedObject value) {
    return writeRecord(key, RecordType.EVENT, intent, value, noop);
  }

  @Override
  public long writeFollowUpEvent(
      long key, Intent intent, UnpackedObject value, Consumer<RecordMetadata> metadata) {
    return writeRecord(key, RecordType.EVENT, intent, value, metadata);
  }

  @Override
  public long writeRejection(
      TypedRecord<? extends UnpackedObject> command, RejectionType rejectionType, String reason) {
    return writeRecord(
        command.getKey(),
        RecordType.COMMAND_REJECTION,
        command.getMetadata().getIntent(),
        rejectionType,
        reason,
        command.getValue(),
        noop);
  }

  @Override
  public long writeRejection(
      TypedRecord<? extends UnpackedObject> command,
      RejectionType rejectionType,
      String reason,
      Consumer<RecordMetadata> metadata) {
    return writeRecord(
        command.getKey(),
        RecordType.COMMAND_REJECTION,
        command.getMetadata().getIntent(),
        rejectionType,
        reason,
        command.getValue(),
        metadata);
  }

  @Override
  public TypedBatchWriter addNewCommand(Intent intent, UnpackedObject value) {
    return addRecord(-1, RecordType.COMMAND, intent, value, noop);
  }

  @Override
  public TypedBatchWriter addFollowUpCommand(long key, Intent intent, UnpackedObject value) {
    return addRecord(key, RecordType.COMMAND, intent, value, noop);
  }

  @Override
  public TypedBatchWriter addNewEvent(Intent intent, UnpackedObject value) {
    return addRecord(-1, RecordType.EVENT, intent, value, noop);
  }

  @Override
  public TypedBatchWriter addFollowUpEvent(long key, Intent intent, UnpackedObject value) {
    return addRecord(key, RecordType.EVENT, intent, value, noop);
  }

  @Override
  public TypedBatchWriter addFollowUpEvent(
      long key, Intent intent, UnpackedObject value, Consumer<RecordMetadata> metadata) {
    return addRecord(key, RecordType.EVENT, intent, value, metadata);
  }

  private TypedBatchWriter addRecord(
      long key,
      RecordType type,
      Intent intent,
      UnpackedObject value,
      Consumer<RecordMetadata> additionalMetadata) {
    initMetadata(type, intent, value);
    additionalMetadata.accept(metadata);

    final LogEntryBuilder logEntryBuilder = batchWriter.event();

    if (key >= 0) {
      logEntryBuilder.key(key);
    } else {
      logEntryBuilder.positionAsKey();
    }

    logEntryBuilder.metadataWriter(metadata).valueWriter(value).done();

    return this;
  }

  @Override
  public long write() {
    return batchWriter.tryWrite();
  }

  @Override
  public TypedBatchWriter newBatch() {
    batchWriter.reset();
    batchWriter.producerId(producerId);

    if (sourceRecordPosition >= 0) {
      batchWriter.sourceRecordPosition(sourceRecordPosition);
    }

    return this;
  }
}
