package org.apache.nifi.reporting.datadog;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.datadog.metrics.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"reporting", "datadog", "metrics"})
@CapabilityDescription("Publishes metrics from NiFi to datadog")
public class DataDogReportingTask extends AbstractReportingTask {


    private MetricsService metricsService = new MetricsService();
    private DDMetricRegistryBuilder ddMetricRegistryBuilder = new DDMetricRegistryBuilder();
    private MetricRegistry metricRegistry = new MetricRegistry();
    private ConcurrentHashMap<String, AtomicLong> metricsMap = new ConcurrentHashMap<>();
    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        ddMetricRegistryBuilder.setMetricRegistry(metricRegistry)
                .setName("nifi_metrics")
                .build();
    }

    @Override
    public void onTrigger(ReportingContext context) {
        final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();
        final List<ProcessorStatus> processorStatuses = new ArrayList<>();
        populateProcessorStatuses(status, processorStatuses);

        for (final ProcessorStatus processorStatus : processorStatuses) {
            Map<String, String> statusMetrics = metricsService.getProcessorMetrics(processorStatus);
            for (Map.Entry<String, String> entry : statusMetrics.entrySet()) {
                final String metricName = "nifi." + processorStatus.getName() + "." + entry.getKey();
                if (!metricsMap.containsKey(metricName)){
                    metricsMap.put(metricName, new AtomicLong(Long.parseLong(entry.getValue())));
                    metricRegistry.register(metricName, new Gauge<Long>() {
                        @Override
                        public Long getValue() {
                            return metricsMap.get(metricName).get();
                        }
                    });
                }
                metricsMap.get(metricName).set(Long.parseLong(entry.getValue()));
            }
        }
    }

    private void populateProcessorStatuses(final ProcessGroupStatus groupStatus, final List<ProcessorStatus> statuses) {
        statuses.addAll(groupStatus.getProcessorStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateProcessorStatuses(childGroupStatus, statuses);
        }
    }
}
