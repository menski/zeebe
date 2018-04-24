package io.zeebe.client.api.events;

import io.zeebe.client.api.record.TopicSubscriberRecord;

public interface TopicSubscriberEvent extends TopicSubscriberRecord
{
    /**
     * @return the current state
     */
    TopicSubscriberState getState();

    enum TopicSubscriberState
    {
        SUBSCRIBED
    }
}
