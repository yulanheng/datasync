package org.fire.datasync.executor;

import org.fire.datasync.common.Lifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * User: fire
 * Date: 2018-01-13
 */
public class Schedulers implements Lifecycle {
    public static final Schedulers INSTANCE = new Schedulers();

    private static ScheduledExecutorService ses;
    private static List<ScheduledFuture> runningTasks;

    private Schedulers() {
        runningTasks = new ArrayList<>();
        ThreadFactory threadFactory = new NamedThreadFactory("schedulers");
        ses = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        close();
    }

    /**
     * 关闭任务调度器
     */
    public void close() {
        for (ScheduledFuture future : runningTasks) {
            future.cancel(false);
        }
        ses.shutdownNow();
    }

    /**
     * 添加调度任务，请注意任何未捕获的异常都将导致
     * 任务终止后续调度
     *
     * @param task     调度任务
     * @param delay    首次执行延时
     * @param period   任务执行间隔
     * @param timeUnit 间隔时间单位
     */
    public void schedule(Runnable task, int delay, int period, TimeUnit timeUnit) {
        runningTasks.add(ses.scheduleWithFixedDelay(task, delay, period, timeUnit));
    }
}
