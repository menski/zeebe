package io.zeebe.client.api.commands;

import java.util.List;

import io.zeebe.transport.SocketAddress;

public interface BrokerInfo
{
    /**
     * @return the address (host + port) of the broker
     */
    SocketAddress getSocketAddress();

    /**
     * @return all partitions of the broker
     */
    List<PartitionInfo> getPartitions();
}
