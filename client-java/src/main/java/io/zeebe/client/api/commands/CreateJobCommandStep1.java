package io.zeebe.client.api.commands;

import java.io.InputStream;
import java.util.Map;

import io.zeebe.client.api.events.JobEvent;

public interface CreateJobCommandStep1
{
    int DEFAULT_RETRIES = 3;

    /**
     * Set the type of the job (i.e. its category). This type is used when open
     * a job subscription to work on this jobs.
     *
     * @param type
     *            the type of this job (e.g. "my-todos")
     * @return the builder for this command
     */
    CreateJobCommandStep2 jobType(String type);

    public interface CreateJobCommandStep2 extends FinalCommandStep<JobEvent>
    {
        /**
         * Add the key-value-pair to the custom headers.
         *
         * @param key
         *            the key of the header
         * @param value
         *            the value of the header
         *
         * @return the builder for this command. Call {@link #send()} to
         *         complete the command and send it to the broker.
         */
        CreateJobCommandStep2 addCustomHeader(String key, Object value);

        /**
         * Add the key-value-pairs to the custom headers.
         *
         * @param headers
         *            the additional custom headers
         *
         * @return the builder for this command. Call {@link #send()} to
         *         complete the command and send it to the broker.
         */
        CreateJobCommandStep2 addCustomHeaders(Map<String, Object> headers);

        /**
         * Set the initial retries of this job. The retries are decremented when
         * the job is marked as failed.
         *
         * @param retries
         *            the initial retries of this job. Must be greater than
         *            zero.
         *
         * @return the builder for this command. Call {@link #send()} to
         *         complete the command and send it to the broker.
         */
        CreateJobCommandStep2 retries(int retries);

        /**
         * Set the initial payload of this job.
         *
         * @param payload
         *            the payload (JSON) as stream
         *
         * @return the builder for this command. Call {@link #send()} to
         *         complete the command and send it to the broker.
         */
        CreateJobCommandStep2 payload(InputStream payload);

        /**
         * Set the initial payload of this job.
         *
         * @param payload
         *            the payload (JSON) as String
         *
         * @return the builder for this command. Call {@link #send()} to
         *         complete the command and send it to the broker.
         */
        CreateJobCommandStep2 payload(String payload);
    }
}
