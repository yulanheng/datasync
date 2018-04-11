package org.fire.datasync.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class SingleThreadExecutor implements Executor {
    private static final Logger log = LoggerFactory.getLogger(SingleThreadExecutor.class);
    private BlockingQueue<Runnable> taskQueue;
    private int maxAllowedTaskCount;
    private Thread thread;
    private String name;
    private volatile boolean running;
    private ThreadFactory threadFactory;

    public SingleThreadExecutor(String name, int capacity) {
        this.name = name;
        this.maxAllowedTaskCount = capacity;
        this.taskQueue = makeTaskQueue();
        this.threadFactory = new NamedThreadFactory(name);
        this.running = true;
        this.thread = threadFactory.newThread(() -> processTaskQueue());
        this.thread.start();
    }

    protected BlockingQueue<Runnable> makeTaskQueue() {
        return new LinkedBlockingQueue<>(maxAllowedTaskCount);
    }

    public void stop() {
        running = false;
        try {
            // oplog以tail方式运行，如果当前没有数据可读则会一直阻塞，
            // 所以这里需要手动interrupt以中断当前行为
            thread.interrupt();
            thread.join();
        } catch (InterruptedException ie) {
            // ignore
        }
    }

    private void processTaskQueue() {
        while (running) {
            try {
                Runnable task = taskQueue.poll();
                if (task != null) {
                    task.run();
                } else {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            } catch (Throwable t) {
                log.error("任务失败，队列名:{}", name, t);
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
        int tryCount = 0;
        int waitMillis = 1000;
        while (true) {
            try {
                if (taskQueue.offer(command, waitMillis, TimeUnit.MILLISECONDS)) {
                    break;
                }
            } catch (InterruptedException ie) {
                // ignore
            }
            tryCount++;
            log.warn("任务线程队列超时，队列名:{}，超时次数:{}", name, tryCount);
        }
    }
}
