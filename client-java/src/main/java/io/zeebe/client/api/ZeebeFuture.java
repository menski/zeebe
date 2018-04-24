package io.zeebe.client.api;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface ZeebeFuture<T> extends Future<T>
{

    /**
     * Like {@link #get()} but throws runtime exceptions.
     */
    T join();

    /**
     * Like {@link #get(long, java.util.concurrent.TimeUnit)} but throws runtime exceptions.
     */
    T join(long timeout, TimeUnit unit);
}
