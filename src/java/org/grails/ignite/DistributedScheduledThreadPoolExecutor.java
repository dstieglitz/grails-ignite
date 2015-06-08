package org.grails.ignite;

import org.apache.ignite.Ignite;

import java.util.concurrent.ExecutorService;
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
 *
 */
public class DistributedScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private Ignite ignite;
    private boolean running = true;

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

    private Runnable wrapRunnable(Runnable command) {
        return new IgniteDistributedRunnable(command);
    }

    public void setRunning(boolean trueOrFalse) {
        this.running = trueOrFalse;
    }

    public boolean isRunning() {
        return this.running;
    }

    private class IgniteDistributedRunnable implements Runnable {
        private Runnable theRunnable;

        public IgniteDistributedRunnable(Runnable theRunnable) {
            super();
            this.theRunnable = theRunnable;
        }

        @Override
        public void run() {
            try {
                if (running) {
                    ignite.executorService().submit(theRunnable);
                }
            } catch (Exception e) {
                // LOG IT HERE!!!
                System.err.println("error in executing: " + theRunnable + ". It will no longer be run!");
                e.printStackTrace();

                // and re throw it so that the Executor also gets this error so that it can do what it would
                // usually do
                throw new RuntimeException(e);
            }
        }
    }
}