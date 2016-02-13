package org.grails.ignite

import grails.util.Holders
import org.springframework.util.AntPathMatcher

import javax.servlet.*
import javax.servlet.http.HttpServletRequest

/**
 * Created by dstieglitz on 9/1/15.
 */
class WebSessionFilter extends org.apache.ignite.cache.websession.WebSessionFilter {

    def excludes

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

        this.excludes = application.config.ignite.webSessionClusteringPathExcludes
        if (this.excludes instanceof ConfigObject) {
            this.excludes = []
        }

        log.info "excluding the following URIs from web session clustering by configuration: ${this.excludes}"

        if (webSessionClusteringEnabled) {
            if (IgniteStartupHelper.grid == null) {
                log.info "web session clustering is enabled but grid is not started, starting now"
                IgniteStartupHelper.startIgnite();
            }
            log.info "configuring web session clustering for gridName=${configuredGridName}"
            super.init(decorator)
        }
    }

    private boolean shouldExclude(String path) {
        log.debug "shouldExclude '${path}' ?"
        AntPathMatcher pathMatcher = new AntPathMatcher();

        for (String exclude : excludes) {
            log.debug "checking ${exclude}"

            // FIXME use a regex
            if (pathMatcher.match(exclude, path)) {
                log.debug "found excluded prefix ${path}"
                return true
            }
        }

        return false
    }

    @Override
    void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
        log.debug "doFilter ${req}, ${res}, ${chain}"

        def application = Holders.grailsApplication
        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        log.debug "webSessionClusteringEnabled=${webSessionClusteringEnabled}"

        String path = ((HttpServletRequest) req).getServletPath();
        if (webSessionClusteringEnabled && !shouldExclude(path)) {
            log.debug "super.doFilter..."
            super.doFilter(req, res, chain)
        } else {
            log.debug "chain.doFilter..."
            chain.doFilter(req, res)
        }
    }
}
