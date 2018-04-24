package io.zeebe.client.api.commands;

import io.zeebe.client.api.events.JobEvent;

public interface UpdateRetriesJobCommandStep1
{
    /**
     * Set the retries of this job.
     * <p>
     * If the given retries are greater than zero then this job will be
     * picked up again by a job subscription and a related incident will be
     * marked as resolved.
     *
     * @param retries
     *            the retries of this job
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    UpdateRetriesJobCommandStep2 retries(int retries);

    interface UpdateRetriesJobCommandStep2 extends FinalCommandStep<JobEvent>
    {
        // the place for new optional parameters
    }
}
