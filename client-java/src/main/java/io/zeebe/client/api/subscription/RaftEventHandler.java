package io.zeebe.client.api.subscription;

import io.zeebe.client.api.events.RaftEvent;

@FunctionalInterface
public interface RaftEventHandler
{

    void onRaftEvent(RaftEvent raftEvent);
}
