package io.zeebe.client.api.commands;

import io.zeebe.client.api.record.TopicRecord;

public interface TopicCommand extends TopicRecord
{
    /**
     * @return the name of the command
     */
    TopicCommandName getCommandName();

    enum TopicCommandName
    {
        CREATE
    }
}
