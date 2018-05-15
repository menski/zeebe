package io.zeebe.client.api.subscription;

import io.zeebe.client.api.commands.JobCommand;

@FunctionalInterface
public interface JobCommandHandler
{

    void onJobCommand(JobCommand jobCommand) throws Exception;

    default void onJobCommandRejection(JobCommand jobCommand) throws Exception
    {
    };
}
