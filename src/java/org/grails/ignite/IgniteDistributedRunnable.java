package org.grails.ignite;

import org.apache.ignite.lang.IgniteRunnable;
import org.apache.log4j.Logger;

import java.util.concurrent.Future;

/**
 * Created by dstieglitz on 3/30/16.
 */
public class IgniteDistributedRunnable implements IgniteRunnable {

    protected Runnable underlyingRunnable;
    protected DistributedScheduledThreadPoolExecutor executor;
    private final Logger log = Logger.getLogger(getClass().getName());

    public IgniteDistributedRunnable(DistributedScheduledThreadPoolExecutor executor, Runnable scheduledRunnable) {
        super();

        if (executor == null) {
            throw new RuntimeException("Can't create a distributed runnable with a null executor");
        }
        if (scheduledRunnable == null) {
            throw new RuntimeException("Can't create a distributed runnable with a null scheduledRunnable");
        }

        this.underlyingRunnable = scheduledRunnable;
        this.executor = executor;
    }

    @Override
    public void run() {
//            try {
        if (executor.isRunning()) {
            log.trace("run " + underlyingRunnable);
            Future f = executor.submit(underlyingRunnable);
            log.debug("got future " + f);
        } else {
            log.debug("scheduler is disabled, will not run " + underlyingRunnable);
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
