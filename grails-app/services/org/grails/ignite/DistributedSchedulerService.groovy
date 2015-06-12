package org.grails.ignite

import org.apache.ignite.compute.ComputeExecutionRejectedException
import org.grails.ignite.ScheduledRunnable
import org.grails.ignite.SchedulerService

import java.util.concurrent.Future
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class DistributedSchedulerService {

    static transactional = false

    def grid

    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit, String name = null)
    throws Exception {
        log.debug "scheduleAtFixedRate ${command}, ${initialDelay}, ${period}, ${unit}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setInitialDelay(initialDelay);
        scheduledRunnable.setPeriod(period);
        scheduledRunnable.setTimeUnit(unit);

        if (getServiceProxy().isScheduled(scheduledRunnable.getName())) {
            throw new ComputeExecutionRejectedException("Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName());
        }

        def future = getServiceProxy().scheduleAtFixedRate(scheduledRunnable)
        log.debug "getServiceProxy().scheduleAtFixedRate returned future ${future}"
        return future
    }

    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit, String name = null)
    throws Exception {
        log.debug "scheduleWithFixedDelay ${command}, ${initialDelay}, ${delay}, ${unit}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setInitialDelay(initialDelay);
        scheduledRunnable.setDelay(delay);
        scheduledRunnable.setTimeUnit(unit);

        if (getServiceProxy().isScheduled(scheduledRunnable)) {
            throw new ComputeExecutionRejectedException("Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName());
        }

        def future = getServiceProxy().scheduleWithFixedDelay(scheduledRunnable)
        log.debug "getServiceProxy().scheduleWithFixedDelay returned future ${future}"
        return future
    }

    public ScheduledFuture schedule(Runnable command, long delay, TimeUnit unit, String name = null) throws Exception {
        log.debug "scheduleWithFixedDelay ${command}, ${delay}, ${unit}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setInitialDelay(delay);
        scheduledRunnable.setTimeUnit(unit);

        if (getServiceProxy().isScheduled(scheduledRunnable)) {
            throw new ComputeExecutionRejectedException("Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName());
        }

        def future = getServiceProxy().schedule(scheduledRunnable)
        log.debug "getServiceProxy().schedule returned future ${future}"
        return future
    }

    public ScheduledFuture scheduleWithCron(Runnable command, String cronExpression, String name = null) throws Exception {
        log.debug "scheduleWithFixedDelay ${command}, ${cronExpression}"

        ScheduledRunnable scheduledRunnable;
        if (name != null) {
            scheduledRunnable = new ScheduledRunnable(name, command);
        } else {
            scheduledRunnable = new ScheduledRunnable(command);
        }

        scheduledRunnable.setCronString(cronExpression);

        if (name != null && getServiceProxy().isScheduled(name)) {
            throw new ComputeExecutionRejectedException("Won't schedule underlyingRunnable that's already scheduled: " + scheduledRunnable.getName());
        }

        def future = getServiceProxy().scheduleWithCron(scheduledRunnable)
        log.debug "getServiceProxy().schedule returned future ${future}"
        return future
    }

    public Future getFuture(String id) {
//        DistributedSchedulerServiceImpl.ScheduledRunnable d = getServiceProxy().getS
    }

    boolean cancel(String name, boolean interrupt) {
        log.debug "cancel '${name}', ${interrupt}"
        return getServiceProxy().cancel(name, interrupt);
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
        return grid.services().serviceProxy("distributedSchedulerService", SchedulerService.class, false)
    }
}
