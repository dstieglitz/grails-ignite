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
        return getServiceProxy().scheduleWithFixedDelay(command, initialDelay, delay, unit)
    }

    def getServiceProxy() {
        return grid.services().serviceProxy("distributedSchedulerService", SchedulerService.class, false)
    }
}
