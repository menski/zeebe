package io.zeebe.client.api.events;

import io.zeebe.client.api.record.TopicSubscriptionRecord;

public interface TopicSubscriptionEvent extends TopicSubscriptionRecord
{
    /**
     * @return the current state
     */
    TopicSubscriptionState getState();

    enum TopicSubscriptionState
    {
        ACKNOWLEDGED
    }
}
