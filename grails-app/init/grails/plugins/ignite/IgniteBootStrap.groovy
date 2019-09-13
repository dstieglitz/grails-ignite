package grails.plugins.ignite

class IgniteBootStrap {
    def grailsApplication

    def init = { servletContext ->
        def webSessionClusteringEnabled = grailsApplication.config.getProperty("ignite.webSessionClusteringEnabled", Boolean, false)

        log.info "webSessionClustering enabled in config? ${webSessionClusteringEnabled}"
    }

    def destroy = {
        // destroy app
    }
}
