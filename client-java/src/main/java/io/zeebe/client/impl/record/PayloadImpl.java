package io.zeebe.client.impl.record;

import java.io.InputStream;

import com.fasterxml.jackson.databind.JsonNode;
import io.zeebe.client.impl.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.data.MsgPackConverter;

public class PayloadImpl
{
    private final MsgPackConverter msgPackConverter;
    private final ZeebeObjectMapperImpl objectMapper;

    private String json;
    private byte[] msgPack;

    public PayloadImpl(ZeebeObjectMapperImpl objectMapper, MsgPackConverter msgPackConverter)
    {
        this.objectMapper = objectMapper;
        this.msgPackConverter = msgPackConverter;
    }

    public PayloadImpl(PayloadImpl other)
    {
        this.objectMapper = other.objectMapper;
        this.msgPackConverter = other.msgPackConverter;
        this.msgPack = other.msgPack;
        this.json = other.json;
    }

    public String getAsJsonString()
    {
        return json;
    }

    public void setJson(String json)
    {
        this.json = json;
        if (json != null)
        {
            this.msgPack = this.msgPackConverter.convertToMsgPack(json);
        }
        else
        {
            this.msgPack = null;
        }
    }

    public void setJson(InputStream stream)
    {
        if (stream != null)
        {
            setMsgPack(this.msgPackConverter.convertToMsgPack(stream));
        }
        else
        {
            setMsgPack(null);
        }
    }

    public void setMsgPack(byte[] msgPack)
    {
        this.msgPack = msgPack;
        if (msgPack != null)
        {
            this.json = this.msgPackConverter.convertToJson(msgPack);
        }
        else
        {
            this.json = null;
        }
    }

    public void set(JsonNode node)
    {
        final String json = objectMapper.toJson(node);
        setJson(json);
    }

    public byte[] getMsgPack()
    {
        return msgPack;
    }
}
