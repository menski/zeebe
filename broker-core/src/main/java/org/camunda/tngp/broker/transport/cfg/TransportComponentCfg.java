package org.camunda.tngp.broker.transport.cfg;

import org.camunda.tngp.broker.clustering.gossip.config.GossipConfiguration;
import org.camunda.tngp.broker.clustering.management.config.ClusterManagementConfig;
import org.camunda.tngp.broker.system.ComponentConfiguration;
import org.camunda.tngp.broker.system.GlobalConfiguration;

public class TransportComponentCfg extends ComponentConfiguration
{
    public String host = "0.0.0.0";
    public int sendBufferSize = 16;
    public int defaultReceiveBufferSize = 16;

    public SocketBindingCfg clientApi = new SocketBindingCfg();
    public SocketBindingCfg managementApi = new SocketBindingCfg();
    public SocketBindingCfg replicationApi = new SocketBindingCfg();

    public GossipConfiguration gossip = new GossipConfiguration();
    public ClusterManagementConfig management = new ClusterManagementConfig();

    @Override
    public void applyGlobalConfiguration(GlobalConfiguration globalConfig)
    {
        gossip.applyGlobalConfiguration(globalConfig);
        management.applyGlobalConfiguration(globalConfig);
    }

}
