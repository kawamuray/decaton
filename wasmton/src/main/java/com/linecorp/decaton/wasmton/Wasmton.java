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

package com.linecorp.decaton.wasmton;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.ProcessorsBuilder;
import com.linecorp.decaton.processor.TaskExtractor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.ProcessorScope;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;

public class Wasmton {
    public static void main(String[] args) {
        if (args.length < 3) {
            throw new RuntimeException("Usage: Wasmton BOOTSTRAP_SERVERS TOPIC PATH_TO_PROCESSOR_WASM");
        }
        String bootstrapServers = args[0];
        String topic = args[1];
        String wasmPath = args[2];

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wasmton-test");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "wasmton-test");
        TaskExtractor<byte[]> extractor = bytes ->
                new DecatonTask<>(TaskMetadata.builder().build(), bytes, bytes);
        ProcessorSubscription subscription = SubscriptionBuilder
                .newBuilder("wasmton-test")
                .consumerConfig(props)
                .processorsBuilder(
                        ProcessorsBuilder.consuming(topic, extractor)
                                         .thenProcess(() -> new WasmtonProcessor(wasmPath),
                                                      ProcessorScope.THREAD))
                .buildAndStart();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                subscription.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }
}
