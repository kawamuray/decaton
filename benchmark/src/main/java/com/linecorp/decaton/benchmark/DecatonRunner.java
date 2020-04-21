/*
 * Copyright 2020 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.decaton.benchmark;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.ProcessorsBuilder;
import com.linecorp.decaton.processor.Property;
import com.linecorp.decaton.processor.StaticPropertySupplier;
import com.linecorp.decaton.processor.SubscriptionStateListener;
import com.linecorp.decaton.processor.TaskExtractor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.ProcessorScope;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.Value;

public class DecatonRunner implements Runner {
    private static final Map<String, Function<String, Object>> propertyConstructors =
            new HashMap<String, Function<String, Object>>() {{
                put(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS.name(), Integer::parseInt);
                put(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY.name(), Integer::parseInt);
            }};

    private ProcessorSubscription subscription;

    public static class PrintMetricReporter implements MetricsReporter {
        private final List<KafkaMetric> metrics = new ArrayList<>();

        private synchronized void handle(KafkaMetric metric) {
            if (metric.metricName().name().startsWith("commit-latency-")) {
                metrics.add(metric);
            }
        }

        @Override
        public void init(List<KafkaMetric> metrics) {
            metrics.forEach(this::handle);
        }

        @Override
        public void metricChange(KafkaMetric metric) {
            handle(metric);
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
        }

        @Override
        public void close() {
            for (KafkaMetric metric : metrics) {
                System.err.printf("%s: %s\n", metric.metricName().name(), metric.metricValue());
            }
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    @Override
    public void init(Config config, Recording recording, ResourceTracker resourceTracker)
            throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "decaton-benchmark");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "decaton-benchmark");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                          PrintMetricReporter.class.getName());

        List<Property<?>> properties = new ArrayList<>();
        for (Map.Entry<String, String> entry : config.parameters().entrySet()) {
            String name = entry.getKey();
            Function<String, Object> ctor = propertyConstructors.get(name);
            Object value = ctor.apply(entry.getValue());
            Property<?> prop = ProcessorProperties.propertyForName(name, value);
            properties.add(prop);
        }

        CountDownLatch startLatch = new CountDownLatch(1);

        subscription = SubscriptionBuilder
                .newBuilder("decaton-benchmark")
                .consumerConfig(props)
                .properties(StaticPropertySupplier.of(properties))
                .processorsBuilder(
                        ProcessorsBuilder.consuming(config.topic(),
                                                    (TaskExtractor<Task>) bytes -> {
                                                        Task task = config.taskDeserializer()
                                                                          .deserialize(config.topic(), bytes);
                                                        return new DecatonTask<>(
                                                                TaskMetadata.builder().build(), task, bytes);
                                                    })
                                         .thenProcess(
                                                 () -> (ctx, task) -> {
                                                     resourceTracker.track(Thread.currentThread().getId());
                                                     recording.process(task);
                                                 },
                                                 ProcessorScope.THREAD))
                .stateListener(state -> {
                    if (state == SubscriptionStateListener.State.RUNNING) {
                        startLatch.countDown();
                    }
                })
                .build();
        resourceTracker.track(subscription.getId());
        subscription.start();

        startLatch.await();
        Metrics.register(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
    }

    @Override
    public void close() throws Exception {
        System.err.println("Total threads = " + ManagementFactory.getThreadMXBean().getThreadCount());

        Collection<Timer> timers = Metrics.registry().get("decaton.subscription.process.durations")
                                          .tag("subscription", "decaton-benchmark")
                                          .timers();
        DistributionSummary pollCount = Metrics.registry().get("decaton.subscription.poll.records")
                                               .tag("subscription", "decaton-benchmark")
                                               .summaries().iterator().next();
        Counter exhausts = Metrics.registry().get("decaton.partition.queue.exhaust")
                                  .tag("subscription", "decaton-benchmark")
                                  .counter();
        System.err.printf("subscription.poll.records: [%s]\n",
                          Arrays.stream(pollCount.takeSnapshot().percentileValues())
                                .map(p -> String.format("%.1f:%.2f", p.percentile() * 100, p.value()))
                                .collect(Collectors.joining(", ")));
        Counter smallRecords = Metrics.registry().get("decaton.subscription.poll.small.records")
                                               .tag("subscription", "decaton-benchmark")
                                               .counter();
        System.err.printf("subscription.poll.small.records: [%s]\n", smallRecords.count());
        System.err.printf("partition.queue.exhaust: %.2f\n", exhausts.count());

        Counter exhaustsTime = Metrics.registry().get("decaton.partition.queue.exhaust.time")
                                  .tag("subscription", "decaton-benchmark")
                                  .counter();
        System.err.printf("partition.queue.exhaust.time: %d\n",
                          TimeUnit.NANOSECONDS.toMillis((long) exhaustsTime.count()));

        for (Timer timer : timers) {
            System.err.printf("subscription.consumer.poll.time [%s] [%s]\n",
                              timer.getId().getTag("scope"),
                              Arrays.stream(timer.takeSnapshot().percentileValues())
                                    .map(p -> String.format("%.1f:%.2f", p.percentile() * 100,
                                                            p.value(TimeUnit.MILLISECONDS)))
                                    .collect(Collectors.joining(", ")));
        }

        System.err.println("fetch duration = " + TimeUnit.NANOSECONDS.toMillis(ProcessorSubscription.lastFetchTime - ProcessorSubscription.firstFetchTime));

        List<ThreadInfo> threads = new ArrayList<>();
        for (long tid : ManagementFactory.getThreadMXBean().getAllThreadIds()) {
            String name = ManagementFactory.getThreadMXBean().getThreadInfo(tid).getThreadName();
            long cpuTime = ManagementFactory.getThreadMXBean().getThreadCpuTime(tid);
            threads.add(new ThreadInfo(tid, name, cpuTime));
        }
        threads.sort((o1, o2) -> (int) (o2.cpuTime - o1.cpuTime));
        for (ThreadInfo thread : threads) {
            System.err.printf("Thread %d/%s consumed CPU %d\n",
                              thread.tid, thread.name, TimeUnit.NANOSECONDS.toMillis(thread.cpuTime));
        }

        if (subscription != null) {
            subscription.close();
        }
    }

    @Value
    private static class ThreadInfo {
        long tid;
        String name;
        long cpuTime;
    }
}
