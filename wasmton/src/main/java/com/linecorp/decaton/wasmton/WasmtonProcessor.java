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

import static wasmtime.WasmValType.I32;
import static wasmtime.WasmValType.I64;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;

import wasmtime.Engine;
import wasmtime.Extern;
import wasmtime.Func;
import wasmtime.Linker;
import wasmtime.Memory;
import wasmtime.Module;
import wasmtime.Store;
import wasmtime.WasmFunctions;
import wasmtime.wasi.Wasi;
import wasmtime.wasi.WasiConfig;
import wasmtime.wasi.WasiConfig.PreopenDir;

public class WasmtonProcessor implements DecatonProcessor<byte[]> {
    private final Store store;
    private final Linker linker;
    private final Wasi wasi;
    private final Memory mem;
    private final Func pollTaskFunc;
    private final Func runFunc;
    private final AtomicReference<byte[]> currentTask;

    public WasmtonProcessor(String wasmPath) {
        store = new Store();
        linker = new Linker(store);
        WasiConfig config = new WasiConfig(new String[0],
                                           // Mount current dir as root for wasm processor
                                           new PreopenDir[] { new PreopenDir("./", "/") });
        wasi = new Wasi(store, config);
        wasi.addToLinker(linker);
        currentTask = new AtomicReference<>();
        AtomicReference<Memory> memRef = new AtomicReference<>();
        pollTaskFunc = WasmFunctions.wrap(store, I64, I32, I32, (bufAddr, bufLen) -> {
            byte[] task = currentTask.getAndSet(null);
            if (task == null || bufLen < task.length) {
                return -1;
            }
            ByteBuffer buf = memRef.get().buffer();
            buf.position(bufAddr.intValue());
            buf.put(task);
            return task.length;
        });
        linker.define("decaton", "poll_task", Extern.fromFunc(pollTaskFunc));
        try (Engine engine = store.engine();
             Module module = Module.fromFile(engine, wasmPath)) {
            linker.module("", module);
        }
        mem = linker.getOneByName("", "memory").memory();
        memRef.set(mem);
        runFunc = linker.getOneByName("", "run").func();
    }

    @Override
    public void process(ProcessingContext<byte[]> context, byte[] task) throws InterruptedException {
        System.err.println("Processing task; length = " + task.length);
        currentTask.set(task);
        WasmFunctions.consumer(runFunc).accept();
    }

    @Override
    public void close() {
        runFunc.close();
        pollTaskFunc.close();
        mem.close();
        wasi.close();
        linker.close();
        store.close();
    }
}
