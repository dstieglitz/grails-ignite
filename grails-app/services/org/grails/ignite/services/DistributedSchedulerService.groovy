package org.grails.ignite.services

import org.apache.ignite.IgniteException
import org.apache.ignite.compute.ComputeExecutionRejectedException
import org.grails.ignite.NamedRunnable
import org.grails.ignite.SchedulerService

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class DistributedSchedulerService {

    static transactional = false

    def grid

    public ScheduledFuture scheduleAtFixedRate(NamedRunnable command, long initialDelay, long period, TimeUnit unit)
    throws Exception {
        log.debug "scheduleAtFixedRate ${command}, ${initialDelay}, ${period}, ${unit}"
        if (getServiceProxy().isScheduled(command.getName())) {
            throw new ComputeExecutionRejectedException("Won't schedule command that's already scheduled: " + command.getName());
        }
        return getServiceProxy().scheduleAtFixedRate(command, initialDelay, period, unit)
    }

    public ScheduledFuture scheduleWithFixedDelay(NamedRunnable command, long initialDelay, long delay, TimeUnit unit)
    throws Exception {
        log.debug "scheduleWithFixedDelay ${command}, ${initialDelay}, ${delay}, ${unit}"
        if (getServiceProxy().isScheduled(command.getName())) {
            throw new ComputeExecutionRejectedException("Won't schedule command that's already scheduled: " + command.getName());
        }
        return getServiceProxy().scheduleWithFixedDelay(command, initialDelay, delay, unit)
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
