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
package org.apache.nifi.reporting.datadog.metrics;

import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A service used to produce key/value metrics based on a given input.
 */
public class MetricsService {

   //metrics for whole data flow
    public Map<String, String> getMetrics(ProcessGroupStatus status) {
        final Map<String, String> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, String.valueOf(status.getFlowFilesReceived()));
        metrics.put(MetricNames.BYTES_RECEIVED, String.valueOf(status.getBytesReceived()));
        metrics.put(MetricNames.FLOW_FILES_SENT, String.valueOf(status.getFlowFilesSent()));
        metrics.put(MetricNames.BYTES_SENT, String.valueOf(status.getBytesSent()));
        metrics.put(MetricNames.FLOW_FILES_QUEUED, String.valueOf(status.getQueuedCount()));
        metrics.put(MetricNames.BYTES_QUEUED, String.valueOf(status.getQueuedContentSize()));
        metrics.put(MetricNames.BYTES_READ, String.valueOf(status.getBytesRead()));
        metrics.put(MetricNames.BYTES_WRITTEN, String.valueOf(status.getBytesWritten()));
        metrics.put(MetricNames.ACTIVE_THREADS, String.valueOf(status.getActiveThreadCount()));
        metrics.put(MetricNames.TOTAL_TASK_DURATION, String.valueOf(calculateProcessingNanos(status)));
        return metrics;
    }

    //processor - specific metrics
    public Map<String, String> getProcessorMetrics(ProcessorStatus status){
        final Map<String, String> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, String.valueOf(status.getInputCount()));
        metrics.put(MetricNames.FLOW_FILES_SENT, String.valueOf(status.getOutputCount()));
        metrics.put(MetricNames.BYTES_READ, String.valueOf(status.getInputBytes()));
        metrics.put(MetricNames.BYTES_WRITTEN, String.valueOf(status.getOutputBytes()));
        metrics.put(MetricNames.ACTIVE_THREADS, String.valueOf(status.getActiveThreadCount()));
        return metrics;
    }

    // calculates the total processing time of all processors in nanos
    protected long calculateProcessingNanos(final ProcessGroupStatus status) {
        long nanos = 0L;

        for (final ProcessorStatus procStats : status.getProcessorStatus()) {
            nanos += procStats.getProcessingNanos();
        }

        for (final ProcessGroupStatus childGroupStatus : status.getProcessGroupStatus()) {
            nanos += calculateProcessingNanos(childGroupStatus);
        }

        return nanos;
    }


}
