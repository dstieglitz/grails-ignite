package grails.plugins.ignite;

import it.sauronsoftware.cron4j.Predictor;
import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by dstieglitz on 3/30/16.
 */
public class IgniteCronDistributedRunnableScheduledFuture<V> extends IgniteDistributedRunnable implements RunnableScheduledFuture<V> {

    private String cronTaskId;
    private boolean cancelled;

    public IgniteCronDistributedRunnableScheduledFuture(DistributedScheduledThreadPoolExecutor executor, Runnable runnable) {
        super(executor, runnable);
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
        executor.deschedule(cronTaskId);
        this.cancelled = true;
        return true;
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

    public Map toDataMap() {
        Map result = new HashMap();

        if (executor == null) { // FIXME this occurs, but why?!
            result.put("error", "NULL EXECUTOR");
        } else {
            Scheduler cronScheduler = executor.getCronScheduler();
            if (cronScheduler == null) {
                result.put("cronExpression", "NULL SCHEDULER");
            } else {
                SchedulingPattern schedulingPattern = cronScheduler.getSchedulingPattern(cronTaskId);
                String cronExpression;
                Predictor p = null;

                if (schedulingPattern == null) {
                    cronExpression = "NO PATTERN";
                } else {
                    cronExpression = schedulingPattern.toString();
                    p = new Predictor(cronExpression);
                }

                result.put("cronTaskId", cronTaskId);
                result.put("cancelled", cancelled);
                result.put("cronExpression", cronExpression);

                if (p != null) {
                    result.put("nextRun", p.nextMatchingDate());
                }
            }
        }

        return result;
    }
}

