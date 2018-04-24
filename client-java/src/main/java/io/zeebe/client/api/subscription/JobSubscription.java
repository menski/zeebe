package io.zeebe.client.api.subscription;

/**
 * Represents the subscription to work items of a certain type. When a subscription is open,
 * the client continuously receives tasks from the broker and hands them to a registered
 * {@link JobHandler}.
 */
public interface JobSubscription
{
    /**
     * @return true if this subscription is currently active and work items are received for it
     */
    boolean isOpen();

    /**
     * @return true if this subscription is not open and is not in the process of opening or closing
     */
    boolean isClosed();

    /**
     * Closes this subscription and stops receiving new work items.
     * Blocks until all previously received items have been
     * handed to a handler.
     */
    void close();
}
