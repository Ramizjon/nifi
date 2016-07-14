package org.apache.nifi.reporting.datadog;


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AtomicDouble;
import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.processor.util.StandardValidators;
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

@Tags({"reporting", "datadog", "metrics"})
@CapabilityDescription("Publishes metrics from NiFi to datadog")
public class DataDogReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor REPORTING_PERIOD = new PropertyDescriptor.Builder()
            .name("DataDog reporting period")
            .description("The amount of time in seconds between polls")
            .required(true)
            .expressionLanguageSupported(false)
            .defaultValue("10")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    private MetricsService metricsService;
    private DDMetricRegistryBuilder ddMetricRegistryBuilder;
    private MetricRegistry metricRegistry;
    private ConcurrentHashMap<String, AtomicDouble> metricsMap;
    private volatile VirtualMachineMetrics virtualMachineMetrics;
    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        metricsService = getMetricsService();
        ddMetricRegistryBuilder = getMetricRegistryBuilder();
        metricRegistry = getMetricRegistry();
        metricsMap = getMetricsMap();
        virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        ddMetricRegistryBuilder.setMetricRegistry(metricRegistry)
                .setName("nifi_metrics")
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REPORTING_PERIOD);
        return properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();
        final String reportingPeriod = context.getProperty(REPORTING_PERIOD)
                .evaluateAttributeExpressions().getValue();
        final List<ProcessorStatus> processorStatuses = new ArrayList<>();
        populateProcessorStatuses(status, processorStatuses);
        ddMetricRegistryBuilder.setInterval(Long.parseLong(reportingPeriod));
        for (final ProcessorStatus processorStatus : processorStatuses) {
            updateMetrics(metricsService.getProcessorMetrics(processorStatus),
                    Optional.of(processorStatus.getName()));
        }
        updateMetrics(metricsService.getJVMMetrics(virtualMachineMetrics),
                Optional.<String>absent());
        updateMetrics(metricsService.getDataFlowMetrics(status), Optional.<String>absent());
    }

    protected void updateMetrics(Map<String, String> metrics, Optional<String> processorName) {
        for (Map.Entry<String, String> entry : metrics.entrySet()) {
        final String metricName = buildMetricName(processorName, entry.getKey());
            logger.info(metricName + ": " + entry.getValue());
            if (!metricsMap.containsKey(metricName)) {
                metricsMap.put(metricName, new AtomicDouble(Double.parseDouble(entry.getValue())));
                metricRegistry.register(metricName, new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return metricsMap.get(metricName).get();
                    }
                });
            }
            metricsMap.get(metricName).set(Double.parseDouble(entry.getValue()));
        }
    }

    private void populateProcessorStatuses(final ProcessGroupStatus groupStatus, final List<ProcessorStatus> statuses) {
        statuses.addAll(groupStatus.getProcessorStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateProcessorStatuses(childGroupStatus, statuses);
        }
    }

    private String buildMetricName(Optional<String> processorName, String metricName) {
        return "nifi." + processorName.or("flow") + "." + metricName;
    }

    protected MetricsService getMetricsService() {
        return new MetricsService();
    }

    protected DDMetricRegistryBuilder getMetricRegistryBuilder() {
        return new DDMetricRegistryBuilder();
    }

    protected MetricRegistry getMetricRegistry() {
        return new MetricRegistry();
    }

    protected ConcurrentHashMap<String, AtomicDouble> getMetricsMap() {
        return new ConcurrentHashMap<>();
    }
}
