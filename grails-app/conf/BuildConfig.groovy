grails.project.class.dir = "target/classes"
grails.project.test.class.dir = "target/test-classes"
grails.project.test.reports.dir = "target/test-reports"

grails.project.fork = [
    // configure settings for compilation JVM, note that if you alter the Groovy version forked compilation is required
    //  compile: [maxMemory: 256, minMemory: 64, debug: false, maxPerm: 256, daemon:true],

    // configure settings for the test-app JVM, uses the daemon by default
    test: [maxMemory: 768, minMemory: 64, debug: false, maxPerm: 256, daemon:true],
    // configure settings for the run-app JVM
    run: [maxMemory: 768, minMemory: 64, debug: false, maxPerm: 256, forkReserve:false],
    //run: false,
    // configure settings for the run-war JVM
    war: [maxMemory: 768, minMemory: 64, debug: false, maxPerm: 256, forkReserve:false],
    // configure settings for the Console UI JVM
    console: [maxMemory: 768, minMemory: 64, debug: false, maxPerm: 256]
]

def igniteVer = '1.7.0'
def igniteHibernateVer = '1.2.0-incubating'

grails.project.dependency.resolver = "maven" // or ivy
grails.project.dependency.resolution = {
    // inherit Grails' default dependencies
    inherits("global") {
        // uncomment to disable ehcache
        // excludes 'ehcache'
        excludes 'h2'
    }
    log "warn" // log level of Ivy resolver, either 'error', 'warn', 'info', 'debug' or 'verbose'
    repositories {
        grailsCentral()
        mavenLocal()
        mavenCentral()
        // uncomment the below to enable remote dependency resolution
        // from public Maven repositories
        //mavenRepo "http://repository.codehaus.org"
        //mavenRepo "http://download.java.net/maven/2/"
        //mavenRepo "http://repository.jboss.com/maven2/"
    }
    dependencies {
        // specify dependencies here under either 'build', 'compile', 'runtime', 'test' or 'provided' scopes eg.
        // runtime 'mysql:mysql-connector-java:5.1.27'
        compile "com.h2database:h2:1.3.175"
        compile "org.apache.ignite:ignite-core:${igniteVer}"
        compile ("org.apache.ignite:ignite-spring:${igniteVer}") {
            excludes 'spring-core', 'spring-aop', 'spring-beans', 'spring-context', 'spring-expression', 'spring-tx'
        }
        compile "org.apache.ignite:ignite-indexing:${igniteVer}"
        compile("org.apache.ignite:ignite-hibernate:${igniteHibernateVer}") {
            excludes 'hibernate-core'
        }
        compile "org.apache.ignite:ignite-web:${igniteVer}"
        compile "org.apache.ignite:ignite-log4j:${igniteVer}"
        compile "org.apache.ignite:ignite-rest-http:${igniteVer}"
        compile "org.apache.ignite:ignite-aws:${igniteVer}"

        compile 'org.bouncycastle:bcprov-jdk15on:1.52'
        compile group: 'com.google.code.findbugs', name: 'jsr305', version: '3.0.0'
        compile 'com.cedarsoftware:groovy-io:1.1.1'
        compile 'it.sauronsoftware.cron4j:cron4j:2.2.5'
    }

    plugins {
        build(":release:3.0.1",
              ":rest-client-builder:1.0.3") {
            export = false
        }

        build ':tomcat:7.0.54'

        compile ':webxml:1.4.1'

        runtime ":hibernate4:4.3.8.1"
    }
}
