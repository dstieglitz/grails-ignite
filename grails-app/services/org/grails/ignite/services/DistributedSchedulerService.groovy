package org.grails.ignite.services

import org.grails.ignite.SchedulerService

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class DistributedSchedulerService implements SchedulerService {

    static transactional = false

    def grid

    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        log.debug "scheduleAtFixedRate ${command}, ${initialDelay}, ${period}, ${unit}"
        return getServiceProxy().scheduleAtFixedRate(command, initialDelay, period, unit)
    }

    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        log.debug "scheduleWithFixedDelay ${command}, ${initialDelay}, ${delay}, ${unit}"
        return getServiceProxy().scheduleWithFixedDelay(command, initialDelay, delay, unit)
    }

    @Override
    void stopScheduler() {
        getServiceProxy().stopScheduler();
    }

    @Override
    void startScheduler() {
        getServiceProxy().startScheduler();
    }

    @Override
    boolean isSchedulerRunning() {
        return getServiceProxy().isSchedulerRunning();
    }

    @Override
    boolean isScheduled(String id) {
        return getServiceProxy().isScheduled(id)
    }

    private SchedulerService getServiceProxy() {
        return grid.services().serviceProxy("distributedSchedulerService", SchedulerService.class, false)
    }
}
