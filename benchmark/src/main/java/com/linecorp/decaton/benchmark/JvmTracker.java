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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JvmTracker {
    @Value
    @Accessors(fluent = true)
    public static class TrackingValues {
        long count;
        long time;
    }

    private final List<GarbageCollectorMXBean> gcMxBeans;
    private final Map<String, TrackingValues> targets;

    public static JvmTracker create() {
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        Map<String, TrackingValues> targets = new HashMap<>();
        for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
            targets.put(gcMxBean.getName(),
                        new TrackingValues(gcMxBean.getCollectionCount(), gcMxBean.getCollectionTime()));
        }
        return new JvmTracker(gcMxBeans, targets);
    }

    JvmTracker(List<GarbageCollectorMXBean> gcMxBeans, Map<String, TrackingValues> targets) {
        this.gcMxBeans = gcMxBeans;
        this.targets = targets;
    }

    public Map<String, TrackingValues> report() {
        Map<String, TrackingValues> report = new HashMap<>();
        for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
            TrackingValues initValues = targets.get(gcMxBean.getName());
            if (initValues == null) {
                log.warn("Could not collect GC stats for {}", gcMxBean.getName());
                continue;
            }
            TrackingValues values =
                    new TrackingValues(gcMxBean.getCollectionCount() - initValues.count,
                                       gcMxBean.getCollectionTime() - initValues.time);
            report.put(gcMxBean.getName(), values);
        }
        return report;
    }
}
