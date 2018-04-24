package io.zeebe.client.api.commands;

import java.util.List;

public interface Topology
{
    /**
     * @return all (known) brokers of the cluster
     */
    List<BrokerInfo> getBrokers();
}
