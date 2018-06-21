package io.zeebe.broker;

import io.zeebe.util.Loggers;
import io.zeebe.util.allocation.AllocatedDirectBuffer;
import java.nio.ByteBuffer;
import net.bytebuddy.implementation.bind.annotation.This;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

public class MyInterceptor {

  private static final Logger LOG = Loggers.ALLOCATION_LOGGER;

  public static void intercept(@This Object buffer) throws Exception {
    final ByteBuffer byteBuffer = ((UnsafeBuffer) buffer).byteBuffer();

    if (byteBuffer != null && byteBuffer.isDirect())
    {
      final long address = BufferUtil.address(byteBuffer);
      assert !AllocatedDirectBuffer.FREED_BUFFERS.contains(address) : address;
    }
  }

}
