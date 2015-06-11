package org.grails.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>Inspired by the blog post at http://code.nomad-labs.com/2011/12/09/mother-fk-the-scheduledexecutorservice/</p>
 * <p>This class wraps the Java concurrent ScheduledThreadPoolExecutor and submits the underlying runnable to an
 * Ignite compute grid instead of using a local thread pool.</p>
 *
 * @author dstieglitz
 * @author srasul
 * @see http://code.nomad-labs.com/2011/12/09/mother-fk-the-scheduledexecutorservice/
 */
public class DistributedScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private static final Logger log = Logger.getLogger(DistributedScheduledThreadPoolExecutor.class.getName());
    @IgniteInstanceResource
    private Ignite ignite;
    private boolean running = true;

    public DistributedScheduledThreadPoolExecutor(int corePoolSize) {
        super(corePoolSize);
    }

    public DistributedScheduledThreadPoolExecutor(Ignite ignite, int corePoolSize) {
        super(corePoolSize);
        this.ignite = ignite;
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(wrapRunnable(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(wrapRunnable(command), initialDelay, delay, unit);
    }

    public boolean cancel(Runnable runnable, boolean mayInterruptIfRunning) {
        log.debug("cancel "+runnable+","+mayInterruptIfRunning);

        if (mayInterruptIfRunning) {
            // FIXME interrupt the Runnable?
            log.warn("mayInterruptIfRunning is currently ignored");
        }

        // these are ScheduledFutureTasks
        for (Runnable r : getQueue()) {
            log.debug("found queued runnable: "+r);
        }

        return super.remove(runnable);
    }

    private Runnable wrapRunnable(Runnable command) {
        return new IgniteDistributedRunnable((ScheduledRunnable) command);
    }

    public boolean isRunning() {
        return this.running;
    }

    public void setRunning(boolean trueOrFalse) {
        this.running = trueOrFalse;
    }

    private class IgniteDistributedRunnable implements Runnable {
        private ScheduledRunnable scheduledRunnable;

        public IgniteDistributedRunnable(ScheduledRunnable scheduledRunnable) {
            super();
            this.scheduledRunnable = scheduledRunnable;
        }

        @Override
        public void run() {
            try {
                if (running) {
                    ignite.executorService().submit(scheduledRunnable);
                }
            } catch (Exception e) {
                // LOG IT HERE!!!
                System.err.println("error in executing: " + scheduledRunnable + ". It will no longer be run!");
                e.printStackTrace();

                // and re throw it so that the Executor also gets this error so that it can do what it would
                // usually do
                throw new RuntimeException(e);
            }
        }
    }
}