import org.grails.ignite.IgniteStartupHelper

class IgniteBootStrap {
    def grailsApplication
    def queryEntityService

    def init = { servletContext ->
        def webSessionClusteringEnabled = (!(grailsApplication.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && grailsApplication.config.ignite.webSessionClusteringEnabled.equals(true))

        log.info "webSessionClustering enabled in config? ${webSessionClusteringEnabled}"
        //ctx.getBean('distributedSchedulerService').grid = grid
        //ctx.getBean(QUERY_ENTITY_SERVICE_NAME).init();
        //queryEntityService.init();
    }

    def destroy = {
        // destroy app
    }
}