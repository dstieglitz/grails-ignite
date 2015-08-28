class IgniteBootStrap {
    def grailsApplication

    def init = { servletContext ->
        def webSessionClusteringEnabled = (!(grailsApplication.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && grailsApplication.config.ignite.webSessionClusteringEnabled.equals(true))

        log.info "webSessionClustering enabled in config? ${webSessionClusteringEnabled}"
    }

    def destroy = {
        // destroy app
    }
}