package io.zeebe.client.api.commands;

import io.zeebe.client.api.record.TopicSubscriptionRecord;

public interface TopicSubscriptionCommand extends TopicSubscriptionRecord
{
    /**
     * @return the command name
     */
    TopicSubscriptionCommandName getCommandName();

    enum TopicSubscriptionCommandName
    {
        ACKNOWLEDGE
    }
}
