package io.zeebe.broker.clustering.orchestration.topic;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.TOPOLOGY_MANAGER_SERVICE;
import static io.zeebe.broker.clustering.orchestration.ClusterOrchestrationLayerServiceNames.snapshotReplicationService;
import static io.zeebe.broker.transport.TransportServiceNames.clientTransport;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.transport.TransportServiceNames;
import io.zeebe.servicecontainer.*;

public class SnapshotReplicationInstallService implements Service<SnapshotReplicationInstallService>
{
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
