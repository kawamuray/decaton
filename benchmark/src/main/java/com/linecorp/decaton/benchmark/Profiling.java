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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Profiling {
    private static final int PROFILER_CMD_TIMEOUT_SECS = 30;

    private final Path asyncProfilerBin;
    private final List<String> asyncProfilerOpts;

    public Profiling(Path asyncProfilerBin, Collection<String> asyncProfilerOpts) {
        this.asyncProfilerBin = asyncProfilerBin;
        if (asyncProfilerOpts == null) {
            this.asyncProfilerOpts = new ArrayList<>();
        } else {
            this.asyncProfilerOpts = new ArrayList<>(asyncProfilerOpts);
        }
        if (!this.asyncProfilerOpts.contains("-f")) {
            this.asyncProfilerOpts.add("-f");
            this.asyncProfilerOpts.add(outputFileName());
        }
    }

    private static long currentPid() {
        String[] names = ManagementFactory.getRuntimeMXBean().getName().split("@", 2);
        return Long.parseLong(names[0]);
    }

    private static String outputFileName() {
        return "profile-" + currentPid() + ".svg";
    }

    private void exec(String subCommand) {
        List<String> cmd = new ArrayList<>();
        cmd.add(asyncProfilerBin.toString());
        cmd.addAll(asyncProfilerOpts);
        cmd.add(subCommand);
        cmd.add(String.valueOf(currentPid()));
        try {
            Process process = Runtime.getRuntime().exec(cmd.toArray(new String[0]));
            if (!process.waitFor(PROFILER_CMD_TIMEOUT_SECS, TimeUnit.SECONDS)) {
                throw new RuntimeException("timed out waiting async-profiler command");
            }
            if (process.exitValue() != 0) {
                log.error("Async profiler exit with error: {}", readAllOut(process.getErrorStream()));
                throw new RuntimeException("async-profiler exits with error: " + process.exitValue());
            }
        } catch (Exception e) {
            log.error("Failed to run profiler command: {}", cmd, e);
            throw new RuntimeException(e);
        }
    }

    static String readAllOut(InputStream errorStream) {
        StringBuilder out = new StringBuilder();
        BufferedReader bf = new BufferedReader(new InputStreamReader(errorStream));
        String line;
        try {
            while ((line = bf.readLine()) != null) {
                out.append(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toString();
    }

    public void start() {
        log.info("Start profiling execution");
        exec("start");
    }

    private Optional<Path> findOutputPath() {
        for (int i = 0; i < asyncProfilerOpts.size() - 1; i++) {
            if ("-f".equals(asyncProfilerOpts.get(i))) {
                return Optional.of(Paths.get(asyncProfilerOpts.get(i + 1)));
            }
        }
        return Optional.empty();
    }

    public Optional<Path> stop() {
        log.info("Finish profiling execution");
        exec("stop");
        return findOutputPath();
    }
}
