package grails.plugins.ignite

import grails.util.Holders
import org.apache.ignite.compute.ComputeExecutionRejectedException
import org.apache.ignite.compute.ComputeTaskFuture
import org.apache.ignite.lang.IgniteUuid

import java.util.concurrent.Future
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * Grails service that acts as a Facade to the Ignite service proxy configured on the grid. Grails applications can
 * inject this service to leverage the configured distributed scheduler.
 * <p>
 * The schedule* methods below wrap any runnables in a ScheduledRunnable object that contains metadata about the
 * schedule of the item. An example object hierarchy could be
 * <pre>
 *     MyRunnable  <-- runnable that contains actual logic
 *     on schedule*,  ScheduledRunnable -- wraps --> IgniteFeedExecutorJob
 *     schedule* submits to ServiceProxy where IgniteDistributedRunnable -- wraps --> ScheduledRunnable -- wraps --> MyRunnable
 * </p>
 */
class DistributedSchedulerService {

    private static final long DEFAULT_TIMEOUT = 60000;

    static transactional = false

    def grid

    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit, long timeout, String name = null)
            throws Exception {
        log.debug "scheduleAtFixedRate ${command}, ${initialDelay}, ${period}, ${unit}, ${name}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setInitialDelay(initialDelay);
        scheduledRunnable.setPeriod(period);
        scheduledRunnable.setTimeUnit(unit);
        if (timeout == null || timeout < 0) timeout = getConfiguredTimeout()
        scheduledRunnable.setTimeout(timeout);

        if (getServiceProxy().isScheduled(scheduledRunnable.getName())) {
//            throw new ComputeExecutionRejectedException("Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName());
            log.warn "Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName()
            return null
        } else {
            def future = getServiceProxy().scheduleAtFixedRate(scheduledRunnable)
            log.debug "getServiceProxy().scheduleAtFixedRate returned future ${future}"
            return future
        }
    }

    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit, String name = null)
            throws Exception {
        return scheduleAtFixedRate(command, initialDelay, period, unit, getConfiguredTimeout(), name)
    }

    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit, long timeout, String name = null)
            throws Exception {
        log.debug "scheduleWithFixedDelay ${command}, ${initialDelay}, ${delay}, ${unit}, ${name}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setInitialDelay(initialDelay);
        scheduledRunnable.setDelay(delay);
        scheduledRunnable.setTimeUnit(unit);
        if (timeout == null || timeout < 0) timeout = getConfiguredTimeout()
        scheduledRunnable.setTimeout(timeout)

        if (getServiceProxy().isScheduled(scheduledRunnable.getName())) {
//            throw new ComputeExecutionRejectedException("Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName());
            log.warn "Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName()
            return null
        } else {
            def future = getServiceProxy().scheduleWithFixedDelay(scheduledRunnable)
            log.debug "getServiceProxy().scheduleWithFixedDelay returned future ${future}"
            return future
        }
    }

    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit, String name = null)
            throws Exception {
        return scheduleWithFixedDelay(command, initialDelay, delay, unit, getConfiguredTimeout(), name)
    }

    /**
     * @param command
     * @param delay
     * @param unit
     * @param timeout
     * @param name
     * @return
     * @throws Exception
     */
    public ScheduledFuture schedule(Runnable command, long delay, TimeUnit unit, long timeout, String name = null) throws Exception {
        log.debug "schedule ${command}, ${delay}, ${unit}, ${name}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setInitialDelay(delay);
        scheduledRunnable.setTimeUnit(unit);
        if (timeout == null || timeout < 0) timeout = getConfiguredTimeout()
        scheduledRunnable.setTimeout(timeout)

        if (getServiceProxy().isScheduled(scheduledRunnable.getName())) {
//            throw new ComputeExecutionRejectedException("Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName());
            log.warn "Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName()
            return null
        } else {
            def future = getServiceProxy().schedule(scheduledRunnable)
            log.debug "getServiceProxy().schedule returned future ${future}"
            return future
        }
    }

    /**
     * @param command
     * @param delay
     * @param unit
     * @param name
     * @return
     * @throws Exception
     */
    public ScheduledFuture schedule(Runnable command, long delay, TimeUnit unit, String name = null)
            throws Exception {
        return schedule(command, delay, unit, getConfiguredTimeout(), name)
    }

    /**
     * FIXME This method is SYNCHRONOUS despite returning a Future
     * @param command
     * @param timeout
     * @param onThisNode
     * @param name
     * @return
     * @throws Exception
     */
    public Future executeNow(Runnable command, long timeout, boolean onThisNode, String name = null) throws Exception {
        log.debug "schedule ${command}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setInitialDelay(0);
        scheduledRunnable.setTimeUnit(TimeUnit.MILLISECONDS);
        scheduledRunnable.setDelay(1);
        if (timeout == null) timeout = getConfiguredTimeout()
        scheduledRunnable.setTimeout(timeout)

        if (onThisNode) {
            def future = getServiceProxy().executeNowOnThisNode(scheduledRunnable)
            log.debug "getServiceProxy().schedule returned future ${future}"
            return future
        } else {
            def future = getServiceProxy().executeNow(scheduledRunnable)
            log.debug "getServiceProxy().schedule returned future ${future}"
            return future
        }
    }

    /**
     * FIXME This method is SYNCHRONOUS despite returning a Future
     * @param command
     * @param name
     * @return
     * @throws Exception
     */
    public Future executeNow(Runnable command, String name = null) throws Exception {
        return executeNow(command, getConfiguredTimeout(), false, name)
    }

    /**
     * FIXME This method is SYNCHRONOUS despite returning a Future
     * @param command
     * @param name
     * @return
     * @throws Exception
     */
    public Future executeNowOnThisNode(Runnable command, String name = null) throws Exception {
        return executeNow(command, getConfiguredTimeout(), true, name)
    }

    public ScheduledFuture scheduleWithCron(Runnable command, String cronExpression, long timeout, String name = null) throws Exception {
        log.debug "scheduleWithCron ${command}, ${cronExpression}, ${timeout}, ${name}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            throw new ComputeExecutionRejectedException("No name supplied to CRON job while being scheduled. You must supply a name.");
//            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setCronString(cronExpression);
        scheduledRunnable.setTimeout(timeout);

        if (name != null && getServiceProxy().isScheduled(name)) {
//            throw new ComputeExecutionRejectedException("Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName());
            log.warn "Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName()
            return null
        } else {
            def future = getServiceProxy().scheduleWithCron(scheduledRunnable)
            log.debug "getServiceProxy().schedule returned future ${future}"
            return future
        }
    }

    public ScheduledFuture scheduleWithCron(Runnable command, String cronExpression, String name = null) throws Exception {
        return scheduleWithCron(command, cronExpression, ScheduledRunnable.DEFAULT_TIMEOUT, name)
    }

    public Map<IgniteUuid, ComputeTaskFuture> getFutures() {
        if (grid == null) {
            throw new RuntimeException("Grid has not been initialized")
        }
        return grid.compute().activeTaskFutures()
    }

    boolean cancel(String name, boolean interrupt) {
        log.debug "cancel '${name}', ${interrupt}"
        boolean result = getServiceProxy().cancel(name, interrupt);
        log.debug "serviceProxy returned ${result} for cancel"
        return result
    }

    void stopScheduler() {
        getServiceProxy().stopScheduler();
    }

    void startScheduler() {
        getServiceProxy().startScheduler();
    }

    boolean isSchedulerRunning() {
        return getServiceProxy().isSchedulerRunning();
    }

    boolean isScheduled(String id) {
        return getServiceProxy().isScheduled(id)
    }

    private SchedulerService getServiceProxy() {
        if (grid == null) {
            throw new RuntimeException("Grid has not been initialized")
        }
        return grid.services().serviceProxy("distributedSchedulerService", SchedulerService.class, false)
    }

    private long getConfiguredTimeout() {
        if (Holders.grailsApplication.config.ignite.defaultJobTimeout instanceof ConfigObject) return DEFAULT_TIMEOUT;
        else return Holders.grailsApplication.config.ignite.defaultJobTimeout
    }
}
