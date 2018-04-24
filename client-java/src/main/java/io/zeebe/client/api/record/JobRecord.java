package io.zeebe.client.api.record;

import java.time.Instant;
import java.util.Map;

public interface JobRecord extends Record
{

    /**
     * @return the type of the job
     */
    String getType();

    /**
     * @return broker-defined headers associated with this job. For example, if
     *         this job is created in the context of workflow instance, the
     *         header provide context information on which activity is executed,
     *         etc.
     */
    Map<String, Object> getHeaders();

    /**
     * @return user-defined headers associated with this job
     */
    Map<String, Object> getCustomHeaders();

    /**
     * @return the lock owner
     */
    String getLockOwner();

    /**
     * @return remaining retries
     */
    Integer getRetries();

    /**
     * @return the time until when the job is exclusively assigned to this
     *         client.
     */
    Instant getLockExpirationTime();

    /**
     * @return JSON-formatted payload
     */
    String getPayload();
}
