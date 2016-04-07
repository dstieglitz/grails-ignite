package org.grails.ignite;

import it.sauronsoftware.cron4j.Scheduler;
import org.apache.ignite.Ignite;
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
 * @see 'http://code.nomad-labs.com/2011/12/09/mother-fk-the-scheduledexecutorservice/'
 */
public class DistributedScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private final Logger log = Logger.getLogger(getClass().getName());
    private Ignite ignite;
    private boolean running = true;
    private Scheduler cronScheduler;

//    public DistributedScheduledThreadPoolExecutor() {
//        super(5);
//        this.cronScheduler = new Scheduler();
//        this.cronScheduler.start();
//    }
//
//    public DistributedScheduledThreadPoolExecutor(int corePoolSize) {
//        super(corePoolSize);
//        this.cronScheduler = new Scheduler();
//        this.cronScheduler.start();
//    }

    public DistributedScheduledThreadPoolExecutor(Ignite ignite, int corePoolSize) {
        super(corePoolSize);
        this.ignite = ignite;
        this.cronScheduler = new Scheduler();
        this.cronScheduler.start();
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        log.debug("scheduleAtFixedRate " + command + "," + initialDelay + "," + period + "," + unit);
        return super.scheduleAtFixedRate(new IgniteDistributedRunnable(this, command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        log.debug("scheduleWithFixedDelay " + command + "," + initialDelay + "," + delay + "," + unit);
        return super.scheduleWithFixedDelay(new IgniteDistributedRunnable(this, command), initialDelay, delay, unit);
    }

    public ScheduledFuture scheduleWithCron(Runnable command, String cronString) {
        log.debug("scheduleWithCron " + command + " cron string");
        IgniteCronDistributedRunnableScheduledFuture scheduledFuture = new IgniteCronDistributedRunnableScheduledFuture(this, command);
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

        if (runnable instanceof IgniteCronDistributedRunnableScheduledFuture) {
            ((IgniteCronDistributedRunnableScheduledFuture) runnable).cancel(mayInterruptIfRunning);
            return true;
        } else {
            // these are ScheduledFutureTasks
//            for (Runnable r : getQueue()) {
//                log.debug("found queued runnable: " + r);
//            }

            return super.remove(runnable);
        }
    }

    public boolean isRunning() {
        return this.running;
    }

    public void setRunning(boolean trueOrFalse) {
        log.debug("setRunning " + trueOrFalse);
        this.running = trueOrFalse;
    }

    @Override
    public Future submit(Runnable runnable) {
        log.debug("submitting " + runnable + " to ignite executor service");
        return ignite.executorService().submit(runnable);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        log.debug("submitting " + task + "," + result + " to ignite executor service");
        return ignite.executorService().submit(task, result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        log.debug("submitting " + task + " to ignite executor service");
        return ignite.executorService().submit(task);
    }

    public void deschedule(String cronTaskId) {
        cronScheduler.deschedule(cronTaskId);
    }

    public Scheduler getCronScheduler() {
        return cronScheduler;
    }
}