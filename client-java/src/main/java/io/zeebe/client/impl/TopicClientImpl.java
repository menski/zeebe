package io.zeebe.client.impl;

import io.zeebe.client.ZeebeClientConfiguration;
import io.zeebe.client.api.clients.*;
import io.zeebe.client.impl.data.MsgPackConverter;
import io.zeebe.client.impl.subscription.SubscriptionManager;
import io.zeebe.util.EnsureUtil;

public class TopicClientImpl implements TopicClient
{
    private final ZeebeClientImpl client;
    private final String topic;

    public TopicClientImpl(ZeebeClientImpl client, String topic)
    {
        EnsureUtil.ensureNotNullOrEmpty("topic name", topic);

        this.client = client;
        this.topic = topic;
    }

    @Override
    public WorkflowClient workflowClient()
    {
        return new WorkflowsClientImpl(this);
    }

    @Override
    public JobClient jobClient()
    {
        return new JobClientImpl(this);
    }

    @Override
    public SubscriptionClient subscriptionClient()
    {
        return new SubscriptionClientImpl(this);
    }

    public String getTopic()
    {
        return topic;
    }

    public RequestManager getCommandManager()
    {
        return client.getCommandManager();
    }

    public ZeebeObjectMapperImpl getObjectMapper()
    {
        return client.getObjectMapper();
    }

    public MsgPackConverter getMsgPackConverter()
    {
        return client.getMsgPackConverter();
    }

    public SubscriptionManager getSubscriptionManager()
    {
        return client.getSubscriptionManager();
    }

    public ZeebeClientConfiguration getConfiguration()
    {
        return client.getConfiguration();
    }

}
