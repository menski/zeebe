package io.zeebe.client.api.record;

import io.zeebe.client.ZeebeClient;

/**
 * A (generic) record in a partition. A record can be an event, a command or a
 * command rejection.
 */
public interface Record
{
    /**
     * @return the record's metadata, such as the topic and partition it belongs
     *         to
     */
    RecordMetadata getMetadata();

    /**
     * @return the record encoded as JSON. Use {@link ZeebeObjectMapper}
     *         accessible via {@link ZeebeClient#objectMapper()} for
     *         deserialization.
     */
    String toJson();

    /**
     * @return the key of the record. Multiple records can have the same key if
     *         they belongs to the same logical entity. Keys are unique for the
     *         combination of topic, partition and record type.
     */
    long getKey();
}
