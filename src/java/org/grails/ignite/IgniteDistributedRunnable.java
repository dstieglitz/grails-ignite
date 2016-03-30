package org.grails.ignite;

import org.apache.ignite.lang.IgniteRunnable;
import org.apache.log4j.Logger;

/**
 * Created by dstieglitz on 3/30/16.
 */
public class IgniteDistributedRunnable implements IgniteRunnable {

    protected Runnable runnable;
    protected DistributedScheduledThreadPoolExecutor executor;
    private final Logger log = Logger.getLogger(getClass().getName());

    public IgniteDistributedRunnable(DistributedScheduledThreadPoolExecutor executor, Runnable scheduledRunnable) {
        super();
        this.runnable = scheduledRunnable;
        this.executor = executor;
    }

    @Override
    public void run() {
//            try {
        if (executor.isRunning()) {
            log.trace("run " + runnable);
            executor.submit(runnable);
        } else {
            log.debug("scheduler is disabled, will not run " + runnable);
        }
//            } catch (Exception e) {
//                // LOG IT HERE!!!
//                log.error("error in executing: " + runnable + ". It will no longer be run!", e);
//
//                // and re throw it so that the Executor also gets this error so that it can do what it would usually do
//                throw new RuntimeException(e);
//            }
    }
}
