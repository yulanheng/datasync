package org.fire.datasync.executor;

import org.fire.datasync.common.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 单线程执行器，内部维护了一个有界任务队列，如果队列已满则添加任务操作
 * 会一直阻塞直到队列有足够空间
 */
public class SingleThreadExecutor implements Executor, Lifecycle {
    private static final Logger log = LoggerFactory.getLogger(SingleThreadExecutor.class);

    private BlockingQueue<Runnable> taskQueue;
    private int maxAllowedTaskCount;
    private Thread thread;
    private String name;
    private volatile boolean running;

    /**
     * @param name     执行器名称
     * @param capacity 任务队列容量
     */
    public SingleThreadExecutor(String name, int capacity) {
        this.name = name;
        this.maxAllowedTaskCount = capacity;
        this.taskQueue = makeTaskQueue();
        this.thread = new NamedThreadFactory(name).newThread(() -> process());
    }

    protected BlockingQueue<Runnable> makeTaskQueue() {
        return new LinkedBlockingQueue<>(maxAllowedTaskCount);
    }

    @Override
    public void start() {
        this.running = true;
        this.thread.start();
    }

    @Override
    public void stop() {
        running = false;
        try {
            thread.interrupt();
            thread.join();
        } catch (InterruptedException ie) {
            // ignore
        }
    }

    /**
     * 执行器运行主流程，循环从队列中取出任务并执行
     */
    private void process() {
        while (running) {
            try {
                Runnable task = taskQueue.take();
                if (task != null) {
                    task.run();
                }
            } catch (Throwable t) {
                if (running) {
                    log.error("process failed:{}", name, t);
                }
            }
        }
    }

    /**
     * 将任务放入队列，如果当前队列已满则会一直阻塞直到队列可用
     *
     * @param command
     */
    @Override
    public void execute(Runnable command) {
        do {
            try {
                taskQueue.put(command);
                break;
            } catch (InterruptedException ie) {
                log.warn("put is interrupted");
                continue;
            }
        } while (true);
    }
}
