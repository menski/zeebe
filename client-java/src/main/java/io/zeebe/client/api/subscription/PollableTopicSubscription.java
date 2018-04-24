package io.zeebe.client.api.subscription;

public interface PollableTopicSubscription
{

    /**
     * @return true if this subscription currently receives entries
     */
    boolean isOpen();

    /**
     * @return true if this subscription is not open and is not in the process of opening or closing
     */
    boolean isClosed();

    /**
     * Closes the subscription. Blocks until all pending entries have been handled.
     */
    void close();

    /**
     * Handles currently pending entries by invoking the supplied subscriber.
     *
     * @param entryHandler the handler that is invoked for each entry on the topic
     * @return number of handled entries
     */
    int poll(RecordHandler entryHandler);
}
