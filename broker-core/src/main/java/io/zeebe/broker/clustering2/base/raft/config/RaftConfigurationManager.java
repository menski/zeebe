package io.zeebe.broker.clustering2.base.raft.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import io.zeebe.broker.Loggers;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

/**
 * Manages {@link BrokerRaftPersistentStorage} instances.
 * When the broker is started, it loads the stored files.
 * Knows where to put new configuration files when a new raft is started.
 *
 */
public class RaftConfigurationManager extends Actor
{
    private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

    private File configurationStoreDirectory;
    private List<BrokerRaftPersistentStorage> configurations = new ArrayList<>();

    public RaftConfigurationManager(File configurationStoreDirectory)
    {
        this.configurationStoreDirectory = configurationStoreDirectory;
    }

    @Override
    protected void onActorStarting()
    {
        final File[] configFiles = configurationStoreDirectory.listFiles();

        if (configFiles != null && configFiles.length > 0)
        {
            for (int i = 0; i < configFiles.length; i++)
            {
                final String path = configFiles[i].getAbsolutePath();

                try
                {
                    configurations.add(new BrokerRaftPersistentStorage(path));
                }
                catch (Exception e)
                {
                    LOG.error("Could not load persistent raft configuration '" +
                            path + "', this broker will not join raft group.", e);
                }
            }
        }
    }

    public ActorFuture<List<BrokerRaftPersistentStorage>> getConfigurations()
    {
        return actor.call(() -> new ArrayList<>(configurations));
    }

    public ActorFuture<BrokerRaftPersistentStorage> createConfiguration(String logDirectory, DirectBuffer topicName, int partitionId)
    {
        return actor.call(() ->
        {
            final String filename = String.format("%s%s-%d.meta", configurationStoreDirectory.getAbsolutePath(), BufferUtil.bufferAsString(topicName), partitionId);
            final BrokerRaftPersistentStorage storage = new BrokerRaftPersistentStorage(filename);

            storage.setLogDirectory(logDirectory)
                .setTopicName(topicName)
                .setPartitionId(partitionId)
                .save();

            configurations.add(storage);

            return storage;
        });
    }

    public ActorFuture<Void> deleteConfiguration(BrokerRaftPersistentStorage configuration)
    {
        return actor.call(() ->
        {
            configurations.remove(configuration);
            configuration.delete();
        });
    }

    public ActorFuture<Void> close()
    {
        return actor.close();
    }
}
