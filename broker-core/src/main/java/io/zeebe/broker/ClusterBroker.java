/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker;

import static java.lang.Runtime.getRuntime;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.NetworkCfg;
import io.zeebe.util.FileUtil;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.util.Scanner;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.SuperMethodCall;

class Broker1 extends ClusterBroker {
  public static void main(String[] args) throws Exception {
    run(51015, null);
  }
}

class Broker2 extends ClusterBroker {
  public static void main(String[] args) throws Exception {
    run(51025, 51016);
  }
}

class Broker3 extends ClusterBroker {
  public static void main(String[] args) throws Exception {
    run(51035, 51016);
  }
}

public class ClusterBroker {
  private static String tempFolder;

  public static void run(final int clientPort, final Integer contactPoint) throws Exception {
    premain(null, ByteBuddyAgent.install());
    final Broker broker = startDefaultBrokerInTempDirectory(clientPort, contactPoint);

    getRuntime()
        .addShutdownHook(
            new Thread("Broker close Thread") {
              @Override
              public void run() {
                try {
                  broker.close();
                } finally {
                  deleteTempDirectory();
                }
              }
            });

    try (Scanner scanner = new Scanner(System.in)) {
      while (scanner.hasNextLine()) {
        final String nextLine = scanner.nextLine();
        if (nextLine.contains("exit")
            || nextLine.contains("close")
            || nextLine.contains("quit")
            || nextLine.contains("halt")
            || nextLine.contains("shutdown")
            || nextLine.contains("stop")) {
          System.exit(0);
        }
      }
    }
  }

  private static Broker startDefaultBrokerInTempDirectory(
      final int clientPort, final Integer contactPoint) {
    try {
      tempFolder = Files.createTempDirectory("zeebe").toAbsolutePath().normalize().toString();

      final BrokerCfg cfg = new BrokerCfg();

      final NetworkCfg network = cfg.getNetwork();

      network.getClient().setPort(clientPort);
      network.getManagement().setPort(clientPort + 1);
      network.getReplication().setPort(clientPort + 2);

      if (contactPoint == null) {
        cfg.setBootstrap(1);
      } else {
        cfg.getCluster().setInitialContactPoints(new String[] {"localhost:" + contactPoint});
      }

      return new Broker(cfg, tempFolder, null);
    } catch (IOException e) {
      throw new RuntimeException("Could not start broker", e);
    }
  }

  private static void deleteTempDirectory() {
    if (tempFolder != null) {
      try {
        FileUtil.deleteFolder(tempFolder);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void premain(String arg, Instrumentation inst) throws Exception {
    new AgentBuilder.Default()
        .type(named("org.agrona.concurrent.UnsafeBuffer"))
        .transform(
            (builder, typeDescription, classLoader, module) ->
                builder
                    .method(nameStartsWith("get").or(nameStartsWith("put")))
                    .intercept(
                        MethodDelegation.to(MyInterceptor.class).andThen(SuperMethodCall.INSTANCE)))
        .installOn(inst);
  }
}
