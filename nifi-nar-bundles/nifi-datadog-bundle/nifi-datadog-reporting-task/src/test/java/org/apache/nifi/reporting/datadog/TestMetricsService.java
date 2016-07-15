package org.apache.nifi.reporting.datadog;

import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.datadog.metrics.MetricNames;
import org.apache.nifi.reporting.datadog.metrics.MetricsService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestMetricsService {

    private ProcessGroupStatus status;
    private MetricsService metricsService;

    @Before
    public void init() {
        status = new ProcessGroupStatus();
        metricsService = new MetricsService();
        status.setId("1234");
        status.setFlowFilesReceived(5);
        status.setBytesReceived(10000);
        status.setFlowFilesSent(10);
        status.setBytesSent(20000);
        status.setQueuedCount(100);
        status.setQueuedContentSize(1024L);
        status.setBytesRead(60000L);
        status.setBytesWritten(80000L);
        status.setActiveThreadCount(5);
    }

    //test group status metric retreiveing
    @Test
    public void testGetProcessGroupStatusMetrics() {
        ProcessorStatus procStatus = new ProcessorStatus();
        List<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        final Map<String, String> metrics = metricsService.getDataFlowMetrics(status);

        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_RECEIVED));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_RECEIVED));
        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_SENT));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_SENT));
        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_QUEUED));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_QUEUED));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_READ));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_WRITTEN));
        Assert.assertTrue(metrics.containsKey(MetricNames.ACTIVE_THREADS));
    }

    //test processor status metric retreiveing
    @Test
    public void testGetProcessorGroupStatusMetrics() {
        ProcessorStatus procStatus = new ProcessorStatus();
        List<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        final Map<String, String> metrics = metricsService.getProcessorMetrics(procStatus);

        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_READ));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_WRITTEN));
        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_RECEIVED));
        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_SENT));
        Assert.assertTrue(metrics.containsKey(MetricNames.ACTIVE_THREADS));
    }

    //test JVM status metric retreiveing
    @Test
    public void testGetVirtualMachineMetrics() {
        final VirtualMachineMetrics virtualMachineMetrics = VirtualMachineMetrics.getInstance();

        final Map<String, String> metrics = metricsService.getJVMMetrics(virtualMachineMetrics);
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_UPTIME));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_HEAP_USED));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_HEAP_USAGE));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_NON_HEAP_USAGE));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_STATES_RUNNABLE));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_STATES_BLOCKED));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_STATES_TIMED_WAITING));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_STATES_TERMINATED));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_COUNT));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_DAEMON_THREAD_COUNT));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_FILE_DESCRIPTOR_USAGE));
    }

}