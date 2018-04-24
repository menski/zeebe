package io.zeebe.client.api.subscription;

/**
 * Represents the subscription to work items of a certain type. When a subscription is open,
 * the client continuously receives work items from the broker. Such items can be handled by calling
 * the {@link #poll(JobHandler)} method.
 */
public interface PollableJobSubscription
{

    /**
     * @return true if this subscription is currently active and work items are received for it
     */
    boolean isOpen();

    /**
     * Closes the subscription. Blocks until all remaining work items have been handled.
     */
    void close();

    /**
     * Calls the provided {@link JobHandler} for a number of tasks that fulfill the subscriptions definition.
     *
     * @return the number of handled work items
     */
    int poll(JobHandler workItemHandler);
}
