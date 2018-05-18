package io.zeebe.client.data;

import java.time.Instant;
import java.util.Properties;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.events.JobEvent;
import io.zeebe.client.api.record.ZeebeObjectMapper;
import io.zeebe.client.impl.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.data.MsgPackConverter;
import io.zeebe.client.impl.data.PayloadField;
import io.zeebe.client.impl.event.JobEventImpl;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;

public class ZeebeObjectMapperTest
{
    private ZeebeObjectMapper objectMapper;

    @Before
    public void init()
    {
        final ZeebeClient client = ZeebeClient.create(new Properties());

        this.objectMapper = client.objectMapper();
    }


    @Test
    public void shouldSerializeRecordToJson()
    {
        final JobEventImpl jobEvent = new JobEventImpl((ZeebeObjectMapperImpl) objectMapper);
        jobEvent.setPayloadField(new PayloadField((ZeebeObjectMapperImpl) objectMapper, new MsgPackConverter()));
        jobEvent.setIntent(JobIntent.CREATED);
        jobEvent.setHeaders(Maps.newHashMap("defaultHeaderKey", "defaultHeaderVal"));
        jobEvent.setCustomHeaders(Maps.newHashMap("customHeaderKey", "customHeaderVal"));
        jobEvent.setKey(79);
        jobEvent.setLockOwner("foo");
        jobEvent.setLockExpirationTime(Instant.now());
        jobEvent.setPartitionId(StubBrokerRule.TEST_PARTITION_ID);
        jobEvent.setPayload("{\"key\":\"val\"}");
        jobEvent.setRetries(123);
        jobEvent.setTopicName(ClientApiRule.DEFAULT_TOPIC_NAME);
        jobEvent.setType("taskTypeFoo");
        jobEvent.setPosition(456);

        final String json = objectMapper.toJson(jobEvent);

        System.out.println(json);
    }

    @Test
    public void shouldDeserializeRecordFromJson()
    {
        final JobEventImpl jobEvent = new JobEventImpl((ZeebeObjectMapperImpl) objectMapper);
        jobEvent.setPayloadField(new PayloadField((ZeebeObjectMapperImpl) objectMapper, new MsgPackConverter()));
        jobEvent.setIntent(JobIntent.CREATED);
        jobEvent.setHeaders(Maps.newHashMap("defaultHeaderKey", "defaultHeaderVal"));
        jobEvent.setCustomHeaders(Maps.newHashMap("customHeaderKey", "customHeaderVal"));
        jobEvent.setKey(79);
        jobEvent.setLockOwner("foo");
        jobEvent.setLockExpirationTime(Instant.now());
        jobEvent.setPartitionId(StubBrokerRule.TEST_PARTITION_ID);
        jobEvent.setPayload("{\"key\":\"val\"}");
        jobEvent.setRetries(123);
        jobEvent.setTopicName(ClientApiRule.DEFAULT_TOPIC_NAME);
        jobEvent.setType("taskTypeFoo");
        jobEvent.setPosition(456);

        final String json = objectMapper.toJson(jobEvent);

        final JobEvent deserializedJobEvent = objectMapper.fromJson(json, JobEvent.class);

        System.out.println(deserializedJobEvent);

        System.out.println(objectMapper.toJson(deserializedJobEvent));
    }

}
