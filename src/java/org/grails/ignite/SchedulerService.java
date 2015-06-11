package org.grails.ignite;

import org.apache.ignite.IgniteException;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Interface for a distributed scheduler service. The service can schedule objects of type NamedRunnable, which
 * provide a getName() method that returns an identifier for the Runnable. The identifier is used to lookup scheduled
 * tasks for cancellation or retrieving specific results (or exceptions).
 *
 * @author Dan Stieglitz
 */
public interface SchedulerService {

    public ScheduledFuture scheduleAtFixedRate(ScheduledRunnable command);

    public ScheduledFuture scheduleWithFixedDelay(ScheduledRunnable command);

    public ScheduledFuture schedule(ScheduledRunnable command);

    public void stopScheduler();

    public void startScheduler();

    public boolean isSchedulerRunning();

    public boolean isScheduled(String id);

    /**
     * Cancel the task with the specified ID. Returns true if the task was found and the cancel was successful,
     * false otherwise.
     * @param name
     * @param mayInterruptIfRunning
     * @return
     */
    public boolean cancel(String name, boolean mayInterruptIfRunning);

}
