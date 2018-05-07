package io.zeebe.broker.system.deployment.data;

import io.zeebe.map.Long2LongZbMap;

/**
 * workflow key -> deployment position
 *
 */
public class DeploymentPositionByWorkflowKey
{
    private final Long2LongZbMap map = new Long2LongZbMap();

    public long get(long key, long missingValue)
    {
        return map.get(key, missingValue);
    }

    public void set(long key, long value)
    {
        map.put(key, value);
    }

    public Long2LongZbMap getRawMap()
    {
        return map;
    }
}
