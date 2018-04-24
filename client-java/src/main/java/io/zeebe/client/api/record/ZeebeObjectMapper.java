package io.zeebe.client.api.record;

/**
 * (De-) Serialize records from/to JSON.
 */
public interface ZeebeObjectMapper
{

    /**
     * @param record the record to serialize
     * @return a canonical JSON representation of the record. This representation (without modifications)
     * can be used to de-serialize the event via {@link #fromJson(String, Class)}.
     */
    String toJson(Record record);

    /**
     * <p>De-serializes an record from JSON.
     *
     * <p>
     * Example usage:
     * <pre>
     * EntryMapper mapper = ..;
     * String json = ..;
     *
     * JobEvent jobEvent = mapper.fromJson(json, JobEvent.class);
     * </pre>
     *
     * @param json the JSON value to de-serialize
     * @param recordClass the type of record to de-serialize it to. Must match the records's entity type.
     * @return the de-serialized record
     */
    <T extends Record> T fromJson(String json, Class<T> recordClass);
}
