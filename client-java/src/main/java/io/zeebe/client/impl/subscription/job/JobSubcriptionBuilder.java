package io.zeebe.client.impl.subscription.job;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.zeebe.client.ZeebeClientConfiguration;
import io.zeebe.client.api.subscription.*;
import io.zeebe.client.api.subscription.JobSubscriptionBuilderStep1.JobSubscriptionBuilderStep2;
import io.zeebe.client.api.subscription.JobSubscriptionBuilderStep1.JobSubscriptionBuilderStep3;
import io.zeebe.client.cmd.ClientException;
import io.zeebe.client.impl.TopicClientImpl;
import io.zeebe.util.EnsureUtil;

public class JobSubcriptionBuilder implements JobSubscriptionBuilderStep1, JobSubscriptionBuilderStep2, JobSubscriptionBuilderStep3
{
    private final JobSubscriberGroupBuilder subscriberBuilder;

    public JobSubcriptionBuilder(TopicClientImpl client)
    {
        this.subscriberBuilder = new JobSubscriberGroupBuilder(client.getTopic(), client.getSubscriptionManager());

        // apply defaults from configuration
        final ZeebeClientConfiguration configuration = client.getConfiguration();
        this.subscriberBuilder.lockOwner(configuration.getDefaultJobLockOwner());
        this.subscriberBuilder.lockTime(configuration.getDefaultJobLockTime().toMillis());
    }

    @Override
    public JobSubscriptionBuilderStep2 jobType(String type)
    {
        subscriberBuilder.jobType(type);
        return this;
    }

    @Override
    public JobSubscriptionBuilderStep3 lockTime(long lockTime)
    {
        subscriberBuilder.lockTime(lockTime);
        return this;
    }

    @Override
    public JobSubscriptionBuilderStep3 lockTime(Duration lockTime)
    {
        subscriberBuilder.lockTime(lockTime.toMillis());
        return this;
    }

    @Override
    public JobSubscriptionBuilderStep3 lockOwner(String lockOwner)
    {
        subscriberBuilder.lockOwner(lockOwner);
        return this;
    }

    @Override
    public JobSubscriptionBuilderStep3 fetchSize(int fetchSize)
    {
        subscriberBuilder.jobFetchSize(fetchSize);
        return this;
    }

    @Override
    public JobSubscriptionBuilderStep3 handler(JobHandler handler)
    {
        EnsureUtil.ensureNotNull("handler", handler);
        subscriberBuilder.jobHandler(handler);
        return this;
    }

    @Override
    public JobSubscription open()
    {
        final Future<JobSubscriberGroup> subscriberGroup = subscriberBuilder.build();

        try
        {
            return subscriberGroup.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new ClientException("Could not open subscription", e);
        }
    }

}
