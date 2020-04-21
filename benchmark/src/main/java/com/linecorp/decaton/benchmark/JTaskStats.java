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

import sun.jvm.hotspot.oops.OopUtilities;
import sun.jvm.hotspot.runtime.JavaThread;
import sun.jvm.hotspot.runtime.Threads;
import sun.jvm.hotspot.runtime.VM;
import sun.jvm.hotspot.tools.Tool;

public class JTaskStats extends Tool {
    @Override
    public void run() {
        VM vm = VM.getVM();
        Threads threads = vm.getThreads();
        for (JavaThread th = threads.first(); th != null; th = th.next()) {
            if (!th.isJavaThread()) {
                continue;
            }
            long tid = OopUtilities.threadOopGetTID(th.getThreadObj());
            long nid = th.getOSThread().threadId();
            String name = th.getThreadName();
            System.err.printf("ThreadMap %d(%s) = %d\n", tid, name, nid);
        }
    }

    public static void main(String[] args) {
        JTaskStats taskStats = new JTaskStats();
        taskStats.execute(args);
    }
}
