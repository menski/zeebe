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
package io.zeebe.broker.system.metrics;

import io.zeebe.broker.system.configuration.MetricsCfg;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.metrics.MetricsManager;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.SchedulingHints;

public class MetricsFileWriterService implements Service<MetricsFileWriter> {
  private MetricsFileWriter metricsFileWriter;
  private MetricsCfg configuration;

  public MetricsFileWriterService(MetricsCfg cfg) {
    this.configuration = cfg;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    final ActorScheduler scheduler = startContext.getScheduler();
    final MetricsManager metricsManager = startContext.getScheduler().getMetricsManager();

    final String metricsFileName = configuration.getFile();

    metricsFileWriter =
        new MetricsFileWriter(
            configuration.getReportingIntervalDuration(), metricsFileName, metricsManager);
    startContext.async(scheduler.submitActor(metricsFileWriter, SchedulingHints.isIoBound(0)));
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    stopContext.async(metricsFileWriter.close());
  }

  @Override
  public MetricsFileWriter get() {
    return metricsFileWriter;
  }
}
