package org.grails.ignite

import grails.util.Holders

import javax.servlet.*

/**
 * Created by dstieglitz on 9/1/15.
 */
class WebSessionFilter extends org.apache.ignite.cache.websession.WebSessionFilter {

    @Override
    void init(FilterConfig cfg) throws ServletException {
        // get grid name from application configuration
        OverridablePropertyFilterConfigDecorator decorator = new OverridablePropertyFilterConfigDecorator(cfg)
        def application = Holders.grailsApplication

        def configuredGridName = IgniteStartupHelper.DEFAULT_GRID_NAME
        if (!(application.config.ignite.gridName instanceof ConfigObject)) {
            configuredGridName = application.config.ignite.gridName
        }

        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        decorator.overrideInitParameter('IgniteWebSessionsGridName', configuredGridName)

        if (webSessionClusteringEnabled) {
            if (IgniteStartupHelper.grid == null) {
                log.info "web session clustering is enabled but grid is not started, starting now"
                IgniteStartupHelper.startIgnite();
            }
            log.info "configuring web session clustering for gridName=${configuredGridName}"
            super.init(decorator)
        }
    }

    @Override
    void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
        def application = Holders.grailsApplication
        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        if (webSessionClusteringEnabled) {
            super.doFilter(req, res, chain)
        } else {
            chain.doFilter(req, res)
        }
    }
}
