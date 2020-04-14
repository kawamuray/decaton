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

import java.util.function.Consumer;

import lombok.Value;
import lombok.experimental.Accessors;

public interface Execution {
    @Value
    @Accessors(fluent = true)
    class Config {
        String bootstrapServers;
        String topic;
        BenchmarkConfig benchmarkConfig;
    }

    enum Stage {
        READY_WARMUP,
        READY,
        FINISH,
    }

    BenchmarkResult execute(Config config, Consumer<Stage> stageCallback) throws InterruptedException;
}
