package io.zeebe.broker.clustering.base;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.TOPOLOGY_MANAGER_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.snapshotReplicationService;
import static io.zeebe.broker.transport.TransportServiceNames.clientTransport;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.transport.TransportServiceNames;
import io.zeebe.servicecontainer.*;
import org.slf4j.Logger;

public class SnapshotReplicationInstallService implements Service<SnapshotReplicationInstallService>
{
    private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

    private ServiceGroupReference<Partition> followerPartitionsGroupReference = ServiceGroupReference.<Partition>create()
            .onAdd(this::onPartitionAdded)
            .build();

    private ServiceStartContext startContext;

    @Override
    public void start(ServiceStartContext startContext)
    {
        this.startContext = startContext;
    }

    public void stop(ServiceStopContext stopContext)
    {
    }

    @Override
    public SnapshotReplicationInstallService get()
    {
        return this;
    }

    private void onPartitionAdded(final ServiceName<Partition> partitionServiceName, final Partition partition)
    {
        final SnapshotReplicationService service = new SnapshotReplicationService();

        LOG.debug("Installing snapshot replication service for {}", partition.getInfo());
        startContext.createService(snapshotReplicationService(partition), service)
                .dependency(partitionServiceName, service.getPartitionInjector())
                .dependency(TOPOLOGY_MANAGER_SERVICE, service.getTopologyManagerInjector())
                .dependency(clientTransport(TransportServiceNames.MANAGEMENT_API_CLIENT_NAME), service.getManagementClientApiInjector())
                .install();
    }

    public ServiceGroupReference<Partition> getFollowerPartitionsGroupReference()
    {
        return followerPartitionsGroupReference;
    }
}
