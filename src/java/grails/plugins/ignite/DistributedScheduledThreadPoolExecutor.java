package grails.plugins.ignite;

import it.sauronsoftware.cron4j.Scheduler;
import org.apache.ignite.Ignite;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
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

    private static final Logger log = Logger.getLogger(DistributedScheduledThreadPoolExecutor.class.getName());
    private Ignite ignite;
    private boolean running = true;
    private Scheduler cronScheduler;
    private long timeout = 60000;
    private TaskDecorator taskDecorator;

    public DistributedScheduledThreadPoolExecutor(Ignite ignite, int corePoolSize) {
        super(corePoolSize);
        this.ignite = ignite;
        this.cronScheduler = new Scheduler();
        this.cronScheduler.start();
    }

    public void setTaskDecorator(TaskDecorator taskDecorator) {
        this.taskDecorator = taskDecorator;
    }

    /**
     * Modifies or replaces the task used to execute a runnable.
     * This method can be used to override the concrete
     * class used for managing internal tasks.
     * The default implementation simply returns the given task.
     *
     * @param runnable the submitted Runnable
     * @param task     the task created to execute the runnable
     * @return a task that can execute the runnable
     * @since 1.6
     */
    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(
            Runnable runnable, RunnableScheduledFuture<V> task) {
        RunnableScheduledFuture<V> r_task;
        if (taskDecorator != null) {
            return taskDecorator.decorateTask(runnable, task);
        }

        return task;
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        log.debug("scheduleAtFixedRate " + command + "," + initialDelay + "," + period + "," + unit);
//        if (!(command instanceof ScheduledRunnable))
//            throw new IllegalArgumentException("Runnable must be of type ScheduledRunnable for this executor");
        return super.scheduleAtFixedRate(new IgniteDistributedRunnable(this, command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        log.debug("scheduleWithFixedDelay " + command + "," + initialDelay + "," + delay + "," + unit);
//        if (!(command instanceof ScheduledRunnable))
//            throw new IllegalArgumentException("Runnable must be of type ScheduledRunnable for this executor");
        return super.scheduleWithFixedDelay(new IgniteDistributedRunnable(this, command), initialDelay, delay, unit);
    }

    @Override
    public ScheduledFuture schedule(Runnable command, long delay, TimeUnit timeUnit) {
        log.debug("scheduling NOW " + command + "," + delay + "," + timeUnit);
        return super.schedule(new IgniteDistributedRunnable(this, command), delay, timeUnit);
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
        log.info("cancel " + runnable + "," + mayInterruptIfRunning);
        boolean cancelled = false;

        if (mayInterruptIfRunning) {
            // FIXME interrupt the Runnable?
            log.warn("mayInterruptIfRunning is currently ignored");
        }

        if (runnable instanceof IgniteCronDistributedRunnableScheduledFuture) {
            ((IgniteCronDistributedRunnableScheduledFuture) runnable).cancel(mayInterruptIfRunning);
            cancelled = true;
        } else {
            log.debug("super.remove " + runnable);
            cancelled = super.remove(runnable);
        }

        super.purge();
        return cancelled;
    }

    public boolean isRunning() {
        return this.running;
    }

    public void setRunning(boolean trueOrFalse) {
        log.debug("setRunning " + trueOrFalse);
        this.running = trueOrFalse;
    }

    /*
     * IgniteDistributedRunnable will call the methods below with its underlying Runnable
     */

    @Override
    public Future submit(Runnable runnable) {
        log.debug("submitting Runnable " + runnable + " to ignite executor service with timeout=" + timeout);
        try {
            ScheduledRunnable sr = (ScheduledRunnable) runnable;
            List tasks = new ArrayList();
            tasks.add(runnable);
            return (Future) ignite.executorService().invokeAll(tasks, sr.getTimeout(), TimeUnit.MILLISECONDS).get(0);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            return null;
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            return null;
        }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        log.debug("submitting " + task + "," + result + " to ignite executor service");
        try {
            ScheduledRunnable sr = (ScheduledRunnable) task;
            List tasks = new ArrayList();
            tasks.add(task);
            return (Future<T>) ignite.executorService().invokeAll(tasks, sr.getTimeout(), TimeUnit.MILLISECONDS).get(0);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            return null;
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            return null;
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        log.debug("submitting Callable<T> " + task + " to ignite executor service with timeout=" + timeout);
        try {
            ScheduledRunnable sr = (ScheduledRunnable) task;
            List<Callable<T>> tasks = new ArrayList<Callable<T>>();
            tasks.add(task);
            return (Future<T>) ignite.executorService().invokeAll(tasks, sr.getTimeout(), TimeUnit.MILLISECONDS).get(0);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            return null;
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            return null;
        }
    }

    public Future submitToThisNode(Callable scheduledRunnable) {
        log.debug("submitting Runnable " + scheduledRunnable + " to ignite executor service on local node with timeout=" + timeout);
        try {
            ScheduledRunnable sr = (ScheduledRunnable) scheduledRunnable;
            List tasks = new ArrayList();
            tasks.add(scheduledRunnable);
            return (Future) ignite.executorService(ignite.cluster().forLocal()).invokeAll(tasks, sr.getTimeout(), TimeUnit.MILLISECONDS).get(0);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            return null;
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            return null;
        }
    }

    public void deschedule(String cronTaskId) {
        cronScheduler.deschedule(cronTaskId);
    }

    public Scheduler getCronScheduler() {
        return cronScheduler;
    }

}