package io.zeebe.client.api.subscription;

import io.zeebe.client.api.events.JobEvent;

@FunctionalInterface
public interface JobEventHandler
{

    void onJobEvent(JobEvent jobEvent) throws Exception;
}
