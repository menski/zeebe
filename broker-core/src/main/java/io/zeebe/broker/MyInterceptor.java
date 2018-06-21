package io.zeebe.broker;

import io.zeebe.util.allocation.AllocatedDirectBuffer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import net.bytebuddy.implementation.bind.annotation.This;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

public class MyInterceptor {

  public static void intercept(@This Object buffer) throws Exception {
    final ByteBuffer byteBuffer = ((UnsafeBuffer) buffer).byteBuffer();

    if (byteBuffer != null && byteBuffer.isDirect())
    {
      final long address = BufferUtil.address(byteBuffer);
      final Throwable t;
      if ((t = AllocatedDirectBuffer.FREED_BUFFERS.get(address)) != null)
      {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        assert false : "Access to buffer at address " + address + " which was already freed by " + sw;
      }
    }
  }

}
