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

package com.linecorp.decaton.processor.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.Utils.Task;

/**
 * This class is responsible for following portions:
 * - Create and manage configured number of {@link ProcessorUnit}s to parallel-ize processing of tasks received
 *   from single partition.
 * - Route fed task appropriately to one of belonging {@link ProcessorUnit}s, respecting task's key for keeping
 *   process locally and ordering.
 * - Manage lifecycle of {@link DecatonProcessor}s for each {@link ProcessorUnit}s.
 */
public class PartitionProcessor implements AsyncShutdownable {
    private static final Logger logger = LoggerFactory.getLogger(PartitionProcessor.class);

    private final PartitionScope scope;
    private final Processors<?> processors;

    private final List<ProcessorUnit> units;
    private CompletableFuture<Void> cleanupResult;

    private final SubPartitioner subPartitioner;

    // Sharing this limiter object with all processor threads
    // can make processing unfair but it likely gives better overall throughput
    private final RateLimiter rateLimiter;

    public PartitionProcessor(PartitionScope scope, Processors<?> processors) {
        this.scope = scope;
        this.processors = processors;

        // Create units with latest property value.
        //
        // NOTE: If the property value is changed multiple times at short intervals,
        // each partition processor can have different number of units temporarily.
        // But it's not a problem because all partitions will be kept paused until all reload requests done.
        // Let's see this by example:
        //   1. change concurrency from 1 to 5 => start reloading
        //   2. processor 1,2,3 are reloaded with 5
        //   3. change concurrency from 5 to 3 during reloading => request reloading again, so partitions will be kept paused
        //   4. at next subscription loop, all processors are reloaded with 3 again, then start processing
        int concurrency = scope.props().get(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY).value();
        units = new ArrayList<>(concurrency);
        subPartitioner = new SubPartitioner(concurrency);
        rateLimiter = new DynamicRateLimiter(scope.props().get(ProcessorProperties.CONFIG_PROCESSING_RATE));

        try {
            for (int i = 0; i < concurrency; i++) {
                units.add(createUnit(i));
            }
        } catch (RuntimeException e) {
            // If exception occurred in the middle of instantiating processor units, we have to make sure
            // all the previously created units are destroyed before bubbling up the exception.
            try {
                close();
            } catch (Exception e1) {
                logger.warn("failed to cleanup intermediate states", e1);
            }
            throw e;
        }
    }

    // visible for testing
    ProcessorUnit createUnit(int threadId) {
        ThreadScope threadScope = new ThreadScope(scope, threadId);

        ExecutionScheduler scheduler = new ExecutionScheduler(threadScope, rateLimiter);

        TopicPartition tp = scope.topicPartition();
        Metrics metrics = Metrics.withTags(
                "subscription", scope.subscriptionId(),
                "topic", tp.topic(),
                "partition", String.valueOf(tp.partition()),
                "subpartition", String.valueOf(threadId));

        ProcessPipeline<?> pipeline = processors.newPipeline(threadScope, scheduler, metrics);
        return new ProcessorUnit(threadScope, pipeline);
    }

    public void addTask(TaskRequest request) {
        int subPartition = subPartitioner.partitionFor(request.key());
        units.get(subPartition).putTask(request);
    }

    @Override
    public void initiateShutdown() {
        cleanupResult = Utils.runInParallel(
                "PartitionProcessor-Cleanup",
                IntStream.range(0, units.size()).mapToObj(i -> (Task) () -> {
                    units.get(i).close();
                    processors.destroyThreadScope(scope.subscriptionId(), scope.topicPartition(), i);
                }).collect(Collectors.toList()));

        try {
            rateLimiter.close();
        } catch (Exception e) {
            logger.error("Error thrown while closing rate limiter", e);
        }
    }

    @Override
    public void awaitShutdown() throws InterruptedException {
        cleanupResult.join();
    }
}
