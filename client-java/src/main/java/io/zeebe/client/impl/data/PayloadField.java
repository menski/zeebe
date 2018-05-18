package io.zeebe.client.impl.data;

import java.io.InputStream;

public class PayloadField
{
    private final MsgPackConverter msgPackConverter;

    private String json;
    private byte[] msgPack;

    public PayloadField(MsgPackConverter msgPackConverter)
    {
        this.msgPackConverter = msgPackConverter;
    }

    public PayloadField(PayloadField other)
    {
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

    public byte[] getMsgPack()
    {
        return msgPack;
    }

    public void clear()
    {
        this.msgPack = null;
        this.json = null;
    }


}
