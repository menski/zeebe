package io.zeebe.client.api.subscription;

public interface TopicSubscription
{

    /**
     * @return true if this subscription is currently active and events are received for it
     */
    boolean isOpen();

    /**
     * @return true if this subscription is not open and is not in the process of opening or closing
     */
    boolean isClosed();

    /**
     * Closes this subscription and stops receiving new entries.
     * Blocks until all previously received entries have been
     * handed to a handler.
     */
    void close();
}
