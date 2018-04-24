package io.zeebe.client.api.commands;

import java.io.InputStream;

import io.zeebe.client.api.events.JobEvent;

public interface CompleteJobCommandStep1 extends FinalCommandStep<JobEvent>
{

    /**
     * Set the payload to complete the job with.
     *
     * @param payload
     *            the payload (JSON) as stream
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    CompleteJobCommandStep1 payload(InputStream payload);

    /**
     * Set the payload to complete the job with.
     *
     * @param payload
     *            the payload (JSON) as String
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    CompleteJobCommandStep1 payload(String payload);

    /**
     * Complete the job without payload.
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    CompleteJobCommandStep1 withoutPayload();

}
