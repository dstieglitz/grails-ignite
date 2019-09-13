import grails.util.BuildSettings
import grails.util.Environment
import org.springframework.boot.logging.logback.ColorConverter
import org.springframework.boot.logging.logback.WhitespaceThrowableProxyConverter

import java.nio.charset.Charset

conversionRule 'clr', ColorConverter
conversionRule 'wex', WhitespaceThrowableProxyConverter

// See http://logback.qos.ch/manual/groovy.html for details on configuration
appender('STDOUT', ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        charset = Charset.forName('UTF-8')

        pattern =
            '%clr(%d{yyyy-MM-dd HH:mm:ss.SSS}){faint} ' + // Date
                '%clr(%5p) ' + // Log level
                '%clr(---){faint} %clr([%15.15t]){faint} ' + // Thread
                '%clr(%-40.40logger{39}){cyan} %clr(:){faint} ' + // Logger
                '%m%n%wex' // Message
    }
}

def targetDir = BuildSettings.TARGET_DIR
if (Environment.isDevelopmentMode() && targetDir != null) {
    appender("FULL_STACKTRACE", FileAppender) {
        file = "${targetDir}/stacktrace.log"
        append = true
        encoder(PatternLayoutEncoder) {
            pattern = "%level %logger - %msg%n"
        }
    }
    logger("StackTrace", ERROR, ['FULL_STACKTRACE'], false)
}

def logAppenders = ['STDOUT']

root(INFO, logAppenders)

logger('groovy.lang.GroovyClassLoader', TRACE, logAppenders, false)
logger('grails.app.services', DEBUG, logAppenders, false)

logger('org.grails.web.servlet', ERROR, logAppenders, false)  //  controllers
logger('org.grails.web.pages', ERROR, logAppenders, false) //  GSP
logger('org.grails.web.sitemesh', ERROR, logAppenders, false) //  layouts
logger('org.grails.web.mapping.filter', ERROR, logAppenders, false) // URL mapping
logger('org.grails.web.mapping', ERROR, logAppenders, false) // URL mapping
logger('org.grails.commons', ERROR, logAppenders, false) // core / classloading
logger('org.grails.plugins', ERROR, logAppenders, false) // plugins
logger('org.grails.orm.hibernate', ERROR, logAppenders, false) // hibernate integration
logger('org.springframework', ERROR, logAppenders, false)
logger('org.hibernate', ERROR, logAppenders, false)
logger('net.sf.ehcache.hibernate', ERROR, logAppenders, false)

logger('org.apache.ignite', DEBUG, logAppenders, false)
logger('grails.plugins.ignite', DEBUG, logAppenders, false)
