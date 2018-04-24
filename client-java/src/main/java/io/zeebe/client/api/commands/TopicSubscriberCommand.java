package io.zeebe.client.api.commands;

import io.zeebe.client.api.record.TopicSubscriberRecord;

public interface TopicSubscriberCommand extends TopicSubscriberRecord
{
    /**
     * @return the command name
     */
    TopicSubscriberCommandName getCommandName();

    enum TopicSubscriberCommandName
    {
        SUBSCRIBE
    }
}
