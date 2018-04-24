package io.zeebe.client.api.clients;

/**
 * A client to operate on workflows, jobs and subscriptions.
 */
public interface TopicClient
{
    /**
     * A client to
     * <li>deploy a workflow
     * <li>create a workflow instance
     * <li>cancel a workflow instance
     * <li>update the payload of a workflow instance
     *
     * @return a client with access to all workflow-related operations.
     */
    WorkflowClient workflowClient();

    /**
     * A client to
     * <li>create a (standalone) job
     * <li>complete a job
     * <li>mark a job as failed
     * <li>update the retries of a job
     *
     * @return a client with access to all job-related operations.
     */
    JobClient jobClient();

    /**
     * A client to
     * <li>open a topic subscription (e.g. for event processing)
     * <li>open a job subscription (i.e. to work on jobs)
     *
     * @return a client with access to all subscription-related operations.
     */
    SubscriptionClient subscriptionClient();

}
