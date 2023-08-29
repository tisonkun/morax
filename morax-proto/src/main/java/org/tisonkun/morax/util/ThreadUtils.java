/*
 * Copyright 2023 tison <wander4096@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tisonkun.morax.util;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ThreadUtils {
    public static ThreadFactory namedThreadFactory(String prefix) {
        final String nameFormat = prefix + "-%d";
        // fail fast if the format is bad or null
        //noinspection ResultOfMethodCallIgnored
        String.format(nameFormat, 0);

        final AtomicLong count = new AtomicLong(0);
        return r -> {
            final Thread t = new Thread(r);
            t.setName(nameFormat.formatted(count.getAndIncrement()));
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * Wrapper over ScheduledThreadPoolExecutor.
     */
    public static ScheduledExecutorService newDaemonSingleThreadScheduledExecutor(String threadName) {
        final ThreadFactory threadFactory = r -> {
            final Thread t = new Thread(r);
            t.setName(threadName);
            t.setDaemon(true);
            return t;
        };
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
        // By default, a cancelled task is not automatically removed from the work queue until its delay
        // elapses. We have to enable it manually.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public static ThreadPoolExecutor newDaemonSingleThreadExecutor(String threadName) {
        final ThreadFactory threadFactory = r -> {
            final Thread t = new Thread(r);
            t.setName(threadName);
            t.setDaemon(true);
            return t;
        };
        return (ThreadPoolExecutor) Executors.newFixedThreadPool(1, threadFactory);
    }

    /**
     * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
     * unique, sequentially assigned integer.
     */
    public static ThreadPoolExecutor newDaemonFixedThreadPool(int nThreads, String prefix) {
        final ThreadFactory threadFactory = namedThreadFactory(prefix);
        return (ThreadPoolExecutor) Executors.newFixedThreadPool(nThreads, threadFactory);
    }

    public static ThreadPoolExecutor newDaemonCachedThreadPool(String prefix, int maxThreadNumber) {
        return newDaemonCachedThreadPool(prefix, maxThreadNumber, 60);
    }

    /**
     * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
     * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
     */
    public static ThreadPoolExecutor newDaemonCachedThreadPool(
            String prefix, int maxThreadNumber, int keepAliveSeconds) {
        final ThreadFactory threadFactory = namedThreadFactory(prefix);
        final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
                maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
                keepAliveSeconds,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory);
        threadPool.allowCoreThreadTimeOut(true);
        return threadPool;
    }
}
