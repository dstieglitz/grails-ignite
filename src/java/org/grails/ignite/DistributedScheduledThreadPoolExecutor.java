package org.grails.ignite;

import groovy.util.logging.Log4j;
import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.SchedulerListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.log4j.Logger;

import java.util.concurrent.*;

/**
 * <p>Inspired by the blog post at http://code.nomad-labs.com/2011/12/09/mother-fk-the-scheduledexecutorservice/</p>
 * <p>This class wraps the Java concurrent ScheduledThreadPoolExecutor and submits the underlying runnable to an
 * Ignite compute grid instead of using a local thread pool.</p>
 * <p>This class extends the default ScheduledThreadPoolExecutor interface with CRON functionality</p>
 *
 * @author dstieglitz
 * @author srasul
 * @see http://code.nomad-labs.com/2011/12/09/mother-fk-the-scheduledexecutorservice/
 */
@Log4j
public class DistributedScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private static final Logger log = Logger.getLogger(DistributedScheduledThreadPoolExecutor.class.getName());
    @IgniteInstanceResource
    private Ignite ignite;
    private boolean running = true;
    private Scheduler cronScheduler;

    public DistributedScheduledThreadPoolExecutor() {
        super(5);
        this.cronScheduler = new Scheduler();
        this.cronScheduler.start();
    }

    public DistributedScheduledThreadPoolExecutor(int corePoolSize) {
        super(corePoolSize);
        this.cronScheduler = new Scheduler();
        this.cronScheduler.start();
    }

    public DistributedScheduledThreadPoolExecutor(Ignite ignite, int corePoolSize) {
        super(corePoolSize);
        this.ignite = ignite;
        this.cronScheduler = new Scheduler();
        this.cronScheduler.start();
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        log.debug("scheduleAtFixedRate "+command+","+initialDelay+","+period+","+unit);
        return super.scheduleAtFixedRate(new IgniteDistributedRunnable(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        log.debug("scheduleWithFixedDelay "+command+","+initialDelay+","+delay+","+unit);
        return super.scheduleWithFixedDelay(new IgniteDistributedRunnable(command), initialDelay, delay, unit);
    }

    public ScheduledFuture scheduleWithCron(Runnable command, String cronString) {
        log.debug("scheduleWithCron "+command+" cron string");
        IgniteCronDistributedRunnable scheduledFuture = new IgniteCronDistributedRunnable(command);
        String id = cronScheduler.schedule(cronString, scheduledFuture);
        scheduledFuture.setCronTaskId(id);

        // return ScheduledFuture for cron task with embedded id
        return scheduledFuture;
    }

    public boolean cancel(Runnable runnable, boolean mayInterruptIfRunning) {
        log.debug("cancel " + runnable + "," + mayInterruptIfRunning);

        if (mayInterruptIfRunning) {
            // FIXME interrupt the Runnable?
            log.warn("mayInterruptIfRunning is currently ignored");
        }

        if (runnable instanceof IgniteCronDistributedRunnable) {
            ((IgniteCronDistributedRunnable) runnable).cancel(mayInterruptIfRunning);
            return true;
        } else {
            // these are ScheduledFutureTasks
            for (Runnable r : getQueue()) {
                log.debug("found queued runnable: " + r);
            }

            return super.remove(runnable);
        }
    }

    public boolean isRunning() {
        return this.running;
    }

    public void setRunning(boolean trueOrFalse) {
        log.debug("setRunning "+trueOrFalse);
        this.running = trueOrFalse;
    }

    private class IgniteDistributedRunnable implements IgniteRunnable {
        protected Runnable runnable;
        private final Logger log = Logger.getLogger(getClass().getName());

        public IgniteDistributedRunnable(Runnable scheduledRunnable) {
            super();
            this.runnable = scheduledRunnable;
        }

        @Override
        public void run() {
//            try {
                if (running) {
                    log.trace("run "+ runnable);
                    ignite.executorService().submit(runnable);
                } else {
                    log.debug("scheduler is disabled, will not run "+ runnable);
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

    private class IgniteCronDistributedRunnable<V>
            extends IgniteDistributedRunnable
            implements RunnableScheduledFuture<V> {

        private String cronTaskId;
        private boolean cancelled;

        public IgniteCronDistributedRunnable(Runnable runnable) {
            super(runnable);
        }

        @Override
        public boolean isPeriodic() {
            throw new UnsupportedOperationException("Operation not supported (yet)");
        }

        @Override
        public long getDelay(TimeUnit unit) {
            throw new UnsupportedOperationException("Operation not supported (yet)");
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (cronTaskId == null) {
                throw new IllegalArgumentException("Can't cancel a task without a cron task ID");
            }
            cronScheduler.deschedule(cronTaskId);
            cancelled = true;
            return isCancelled();
        }

        @Override
        public boolean isCancelled() {
            return this.cancelled;
        }

        @Override
        public boolean isDone() {
            throw new UnsupportedOperationException("Operation not supported (yet)");
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException("Operation not supported (yet)");
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException("Operation not supported (yet)");
        }

        public String getCronTaskId() {
            return this.cronTaskId;
        }

        public void setCronTaskId(String cronTaskId) {
            this.cronTaskId = cronTaskId;
        }
    }
}