package io.zeebe.client.impl.data;

import java.io.IOException;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class PayloadModule extends SimpleModule
{
    private static final long serialVersionUID = 1L;

    public PayloadModule(MsgPackConverter msgPackConverter)
    {
        addSerializer(PayloadField.class, new PayloadSerializer());
        addDeserializer(PayloadField.class, new PayloadDeserializer(msgPackConverter));
    }

    class PayloadSerializer extends StdSerializer<PayloadField>
    {

        private static final long serialVersionUID = 1L;

        protected PayloadSerializer()
        {
            this(null);
        }

        protected PayloadSerializer(Class<PayloadField> t)
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

    class PayloadDeserializer extends StdDeserializer<PayloadField>
    {
        private static final long serialVersionUID = 1L;

        private MsgPackConverter msgPackConverter;

        protected PayloadDeserializer(MsgPackConverter msgPackConverter)
        {
            this((Class<?>) null);
            this.msgPackConverter = msgPackConverter;
        }

        protected PayloadDeserializer(Class<?> vc)
        {
            super(vc);
        }

        @Override
        public PayloadField deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            final TreeNode node = p.readValueAsTree();
            final String json = node.toString();

            final PayloadField payload = new PayloadField(msgPackConverter);
            payload.setJson(json);

            return payload;
        }

    }

}
