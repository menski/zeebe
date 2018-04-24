package io.zeebe.client.api.commands;

import io.zeebe.client.api.events.JobEvent;

public interface FailJobCommandStep1
{

    /**
     * Set the remaining retries of this job.
     * <p>
     * If the retries are greater than zero then this job will be picked up
     * again by a job subscription. Otherwise, an incident is created for
     * this job.
     *
     * @param remaingRetries
     *            the remaining retries of this job (e.g.
     *            "jobEvent.getRetries() - 1")
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    FailJobCommandStep2 retries(int remaingRetries);

    interface FailJobCommandStep2 extends FinalCommandStep<JobEvent>
    {
        // the place for new optional parameters
    }

}
