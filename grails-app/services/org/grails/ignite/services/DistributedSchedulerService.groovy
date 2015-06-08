package org.grails.ignite.services

import java.util.concurrent.TimeUnit

class DistributedSchedulerService {

    static transactional = false

    def grid

    def scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        log.debug "scheduleAtFixedRate ${command}, ${initialDelay}, ${period}, ${unit}"
        return getServiceProxy().scheduleAtFixedRate(command, initialDelay, period, unit)
    }

    def scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return getServiceProxy().scheduleWithFixedDelay(command, initialDelay, delay, unit)
    }

    def getServiceProxy() {
        return grid.services().serviceProxy("distributedSchedulerService", org.grails.ignite.DistributedSchedulerService.class, false)
    }
}
