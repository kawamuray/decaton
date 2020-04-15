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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.linecorp.decaton.benchmark.BenchmarkResult.Performance;
import com.linecorp.decaton.benchmark.BenchmarkResult.ResourceUsage;
import com.linecorp.decaton.benchmark.ResourceTracker.TrackingValues;
import com.linecorp.decaton.benchmark.Task.KafkaDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InProcessExecution implements Execution {
    @Override
    public BenchmarkResult execute(Config config, Consumer<Stage> stageCallback) throws InterruptedException {
        BenchmarkConfig bmConfig = config.benchmarkConfig();

        log.info("Loading runner {}", bmConfig.runner());
        Runner runner = Runner.fromClassName(bmConfig.runner());
        Recording recording = new Recording(bmConfig.tasks(), bmConfig.warmupTasks());
        ResourceTracker resourceTracker = new ResourceTracker();
        log.info("Initializing runner {}", bmConfig.runner());

        Runner.Config runnerConfig = new Runner.Config(config.bootstrapServers(),
                                                       config.topic(),
                                                       new KafkaDeserializer(),
                                                       bmConfig.params());
        Profiling profiling = null;
        if (bmConfig.profiling() != null) {
            profiling = new Profiling(bmConfig.profiling().profilerBin() ,bmConfig.profiling().profilerOpts());
        }
        runner.init(runnerConfig, recording, resourceTracker);
        final Map<Long, TrackingValues> resourceUsageReport;
        try {
            stageCallback.accept(Stage.READY_WARMUP);
            if (!recording.awaitWarmupComplete(3, TimeUnit.MINUTES)) {
                throw new RuntimeException("timeout on awaiting benchmark to complete");
            }
            if (profiling != null) {
                profiling.start();
            }

            Thread.sleep(3000);
            stageCallback.accept(Stage.READY);
            if (!recording.await(3, TimeUnit.MINUTES)) {
                throw new RuntimeException("timeout on awaiting benchmark to complete");
            }
            if (profiling != null) {
                profiling.stop().ifPresent(
                        outputPath -> log.info("Profiling output is available at {}", outputPath));
            }
            resourceUsageReport = resourceTracker.report();
        } finally {
            try {
                runner.close();
            } catch (Exception e) {
                log.warn("Failed to close runner - {}", runner.getClass(), e);
            }
        }

        stageCallback.accept(Stage.FINISH);
        return assembleResult(recording, resourceUsageReport);
    }

    private static BenchmarkResult assembleResult(Recording recording,
                                                  Map<Long, TrackingValues> resourceUsageReport) {
        Performance performance = recording.computeResult();
        int threads = resourceUsageReport.size();
        TrackingValues resourceValues =
                resourceUsageReport.values().stream()
                                   .reduce(new TrackingValues(0, 0),
                                           (a, b) -> new TrackingValues(
                                                   a.cpuTime() + b.cpuTime(),
                                                   a.allocatedBytes() + b.allocatedBytes()));
        ResourceUsage resource = new ResourceUsage(threads,
                                                   resourceValues.cpuTime(),
                                                   resourceValues.allocatedBytes());
        return new BenchmarkResult(performance, resource);
    }
}
