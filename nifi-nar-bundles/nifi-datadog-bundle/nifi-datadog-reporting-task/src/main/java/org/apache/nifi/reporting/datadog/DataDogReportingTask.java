/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.reporting.datadog;


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AtomicDouble;
import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.datadog.metrics.MetricsService;
import org.coursera.metrics.datadog.DynamicTagsCallback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Tags({"reporting", "datadog", "metrics"})
@CapabilityDescription("Publishes metrics from NiFi to datadog")
public class DataDogReportingTask extends AbstractReportingTask {

    static final AllowableValue DATADOG_AGENT = new AllowableValue("DataDog Agent", "DataDog UDP Agent",
            "Metrics will be sent via locally installed DataDog agent over UDP");

    static final AllowableValue DATADOG_HTTP = new AllowableValue("Datadog HTTP", "Datadog HTTP",
            "Metrics will be sent via HTTP transport with no need of Agent installed. " +
                    "DataDog API key needs to be set");

    static final PropertyDescriptor DATADOG_TRANSPORT = new PropertyDescriptor.Builder()
            .name("DataDog transport")
            .description("Transport through which metrics will be sent to DataDog")
            .required(true)
            .allowableValues(DATADOG_AGENT, DATADOG_HTTP)
            .defaultValue(DATADOG_AGENT.getValue())
            .build();

    static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API key")
            .description("DataDog API key. If specified value is 'agent', local DataDog agent will be used.")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor METRICS_PREFIX = new PropertyDescriptor.Builder()
            .name("Metrics prefix")
            .description("Prefix to be added before every metric")
            .required(true)
            .expressionLanguageSupported(false)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ENVIRONMENT = new PropertyDescriptor.Builder()
            .name("Environment")
            .description("Environment, dataflow is running in. " +
                    "This property will be included as metrics tag.")
            .required(true)
            .expressionLanguageSupported(false)
            .defaultValue("dev")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private MetricsService metricsService;
    private DDMetricRegistryBuilder ddMetricRegistryBuilder;
    private MetricRegistry metricRegistry;
    private String metricsPrefix;
    private String environment;
    private String statusId;
    private String apiKey;
    private ConcurrentHashMap<String, AtomicDouble> metricsMap;
    private volatile VirtualMachineMetrics virtualMachineMetrics;

    @OnScheduled
    public void setup(final ConfigurationContext context) {
        metricsService = getMetricsService();
        ddMetricRegistryBuilder = getMetricRegistryBuilder();
        metricRegistry = getMetricRegistry();
        metricsMap = getMetricsMap();
        metricsPrefix = METRICS_PREFIX.getDefaultValue();
        environment = ENVIRONMENT.getDefaultValue();
        apiKey = API_KEY.getDefaultValue();
        virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        ddMetricRegistryBuilder.setMetricRegistry(metricRegistry)
                .setName("nifi_metrics")
                .setTags(Arrays.asList("env", "dataflow_id"));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(METRICS_PREFIX);
        properties.add(ENVIRONMENT);
        properties.add(API_KEY);
        properties.add(DATADOG_TRANSPORT);
        return properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();
        metricsPrefix = context.getProperty(METRICS_PREFIX).getValue();
        environment = context.getProperty(ENVIRONMENT).getValue();
        apiKey = context.getProperty(API_KEY).getValue();
        statusId = status.getId();
        try {
            updateDataDogTransport(context);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final List<ProcessorStatus> processorStatuses = new ArrayList<>();
        populateProcessorStatuses(status, processorStatuses);
        for (final ProcessorStatus processorStatus : processorStatuses) {
            updateMetrics(metricsService.getProcessorMetrics(processorStatus),
                    Optional.of(processorStatus.getName()));
        }
        updateMetrics(metricsService.getJVMMetrics(virtualMachineMetrics),
                Optional.<String>absent());
        updateMetrics(metricsService.getDataFlowMetrics(status), Optional.<String>absent());
        //report all metrics to DataDog
        ddMetricRegistryBuilder.getDatadogReporter().report();
    }

    protected void updateMetrics(Map<String, String> metrics, Optional<String> processorName) {
        for (Map.Entry<String, String> entry : metrics.entrySet()) {
            final String metricName = buildMetricName(processorName, entry.getKey());
            //if metric is not registered yet - register it
            if (!metricsMap.containsKey(metricName)) {
                metricsMap.put(metricName, new AtomicDouble(Double.parseDouble(entry.getValue())));
                metricRegistry.register(metricName, new MetricGauge(metricName, environment, statusId));
            }
            //set real time value to metrics map
            metricsMap.get(metricName).set(Double.parseDouble(entry.getValue()));
        }
    }

    private class MetricGauge implements Gauge, DynamicTagsCallback {
        String metricName;
        String environment;
        String dataflowId;

        public MetricGauge(String metricName, String env, String dId) {
            this.metricName = metricName;
            this.environment = env;
            this.dataflowId = dId;
        }

        @Override
        public Object getValue() {
            return metricsMap.get(metricName).get();
        }

        @Override
        public List<String> getTags() {
            return Arrays.asList("env:" + environment, "dataflow_id:" + dataflowId);
        }
    }

    private void updateDataDogTransport(ReportingContext context) throws IOException {
        String dataDogTransport = context.getProperty(DATADOG_TRANSPORT).getValue();
        if (dataDogTransport.equalsIgnoreCase(DATADOG_AGENT.getValue())) {
            ddMetricRegistryBuilder.build("agent");
        } else if (dataDogTransport.equalsIgnoreCase(DATADOG_HTTP.getValue())
                && context.getProperty(API_KEY).isSet()) {
            ddMetricRegistryBuilder.build(context.getProperty(API_KEY).getValue());
        }
    }

    private void populateProcessorStatuses(final ProcessGroupStatus groupStatus, final List<ProcessorStatus> statuses) {
        statuses.addAll(groupStatus.getProcessorStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateProcessorStatuses(childGroupStatus, statuses);
        }
    }

    private String buildMetricName(Optional<String> processorName, String metricName) {
        return metricsPrefix + "." + processorName.or("flow") + "." + metricName;
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