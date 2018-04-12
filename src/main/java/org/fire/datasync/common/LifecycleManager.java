package org.fire.datasync.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 生命周期管理器
 * User: fire
 * Date: 2018-04-12
 */
public class LifecycleManager implements Lifecycle {
    private static final Logger log = LoggerFactory.getLogger(LifecycleManager.class);
    public final static LifecycleManager INSTANCE = new LifecycleManager();

    private List<Lifecycle> lifecycles = new ArrayList<>();

    private LifecycleManager() {
    }

    /**
     * 添加声明周期管理单元，添加顺序将影响启动和停止的顺序
     *
     * @param lifecycle
     */
    public void add(Lifecycle lifecycle) {
        lifecycles.add(lifecycle);
    }

    @Override
    public void start() {
        for (Lifecycle lifecycle : lifecycles) {
            log.info("START {}", lifecycle.getClass().getName());
            lifecycle.start();
        }
    }

    @Override
    public void stop() {
        for (Lifecycle lifecycle : lifecycles) {
            log.info("STOP {}", lifecycle.getClass().getName());
            lifecycle.stop();
        }
    }
}
