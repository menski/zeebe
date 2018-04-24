package io.zeebe.client.impl;

import io.zeebe.client.api.clients.JobClient;
import io.zeebe.client.api.commands.*;
import io.zeebe.client.api.events.JobEvent;
import io.zeebe.client.impl.job.*;

public class JobClientImpl implements JobClient
{
    private final TopicClientImpl client;

    public JobClientImpl(TopicClientImpl client)
    {
        this.client = client;
    }

    @Override
    public CreateJobCommandStep1 newCreateCommand()
    {
        return new CreateJobCommandImpl(client.getCommandManager(), client.getMsgPackConverter(), client.getTopic());
    }

    @Override
    public CompleteJobCommandStep1 newCompleteCommand(JobEvent event)
    {
        return new CompleteJobCommandImpl(client.getCommandManager(), event);
    }

    @Override
    public FailJobCommandStep1 newFailCommand(JobEvent event)
    {
        return new FailJobCommandImpl(client.getCommandManager(), event);
    }

    @Override
    public UpdateRetriesJobCommandStep1 newUpdateRetriesCommand(JobEvent event)
    {
        return new UpdateRetriesJobCommandImpl(client.getCommandManager(), event);
    }

}
