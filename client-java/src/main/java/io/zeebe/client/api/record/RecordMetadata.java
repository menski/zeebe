package io.zeebe.client.api.record;

/**
 * The metadata of a record.
 */
public interface RecordMetadata
{
    /**
     * @return the name of the topic this record is published on
     */
    String getTopicName();

    /**
     * @return the id of the partition this record is published on
     */
    int getPartitionId();

    /**
     * @return the unique position the record has in the partition. Records are
     *         ordered by position.
     */
    long getPosition();

    /**
     * @return the key of the record. Multiple records can have the same key if
     *         they belongs to the same logical entity. Keys are unique for the
     *         combination of topic, partition and record type.
     */
    long getKey();

    /**
     * @return the type of the record (event, command or command rejection)
     */
    RecordType getRecordType();

    /**
     * @return the type of the record (e.g. job, worklow, workflow instance,
     *         etc.)
     */
    ValueType getValueType();

    /**
     * @return either the event or the command name, depending if the record is
     *         an event or a command (rejection)
     */
    String getIntent();

    enum ValueType
    {
        JOB,
        WORKFLOW_INSTANCE,
        INCIDENT,
        SUBSCRIBER,
        SUBSCRIPTION,
        DEPLOYMENT,

        TOPIC,

        RAFT,

        UNKNOWN
    }

    enum RecordType
    {
        EVENT,
        COMMAND,
        COMMAND_REJECTION
    }
}
