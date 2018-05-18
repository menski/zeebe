package io.zeebe.client.impl.data;

import java.io.IOException;
import java.time.Instant;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.zeebe.protocol.Protocol;

public class MsgpackIntentModule extends SimpleModule
{
    private static final long serialVersionUID = 1L;

    public MsgpackIntentModule()
    {
        addSerializer(Instant.class, new MsgpackInstantSerializer());
        addDeserializer(Instant.class, new MsgpackInstantDeserializer());
    }

    class MsgpackInstantSerializer extends StdSerializer<Instant>
    {

        private static final long serialVersionUID = 1L;

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
        private static final long serialVersionUID = 1L;

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

}
