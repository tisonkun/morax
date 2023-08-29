package org.tisonkun.nymph.util;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
    public static ThreadPoolExecutor  newDaemonFixedThreadPool(int nThreads, String prefix) {
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
    public static ThreadPoolExecutor newDaemonCachedThreadPool(String prefix, int maxThreadNumber, int keepAliveSeconds) {
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
