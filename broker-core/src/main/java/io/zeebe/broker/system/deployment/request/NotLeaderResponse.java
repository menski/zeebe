package io.zeebe.broker.system.deployment.request;

import io.zeebe.clustering.management.*;
import io.zeebe.util.buffer.BufferReader;
import io.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class NotLeaderResponse implements BufferReader, BufferWriter
{
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final NotLeaderResponseEncoder bodyEncoder = new NotLeaderResponseEncoder();

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final NotLeaderResponseDecoder bodyDecoder = new NotLeaderResponseDecoder();

    @Override
    public int getLength()
    {
        return headerEncoder.encodedLength() +
            bodyEncoder.sbeBlockLength();
    }

    @Override
    public void write(MutableDirectBuffer buffer, int offset)
    {
        headerEncoder.wrap(buffer, offset)
            .blockLength(bodyEncoder.sbeBlockLength())
            .templateId(bodyEncoder.sbeTemplateId())
            .schemaId(bodyEncoder.sbeSchemaId())
            .version(bodyEncoder.sbeSchemaVersion());

        bodyEncoder.wrap(buffer, offset + headerEncoder.encodedLength());
    }

    public boolean tryWrap(DirectBuffer buffer, int offset, int length)
    {
        headerDecoder.wrap(buffer, offset);

        return headerDecoder.schemaId() == bodyDecoder.sbeSchemaId() &&
            headerDecoder.templateId() == bodyDecoder.sbeTemplateId();
    }

    @Override
    public void wrap(DirectBuffer buffer, int offset, int length)
    {
        headerDecoder.wrap(buffer, offset);

        offset += headerDecoder.encodedLength();

        bodyDecoder.wrap(buffer,
            offset,
            headerDecoder.blockLength(),
            headerDecoder.version());
    }
}
