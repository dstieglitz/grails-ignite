/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.grails.ignite;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.event.ContextRefreshedEvent;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * Ignite Spring bean allows to bypass {@link Ignition} methods.
 * In other words, this bean class allows to inject new grid instance from
 * Spring configuration file directly without invoking static
 * {@link Ignition} methods. This class can be wired directly from
 * Spring and can be referenced from within other Spring beans.
 * By virtue of implementing {@link DisposableBean} and {@link SmartInitializingSingleton}
 * interfaces, {@code IgniteSpringBean} automatically starts and stops underlying
 * grid instance.
 *
 * <p>
 * A note should be taken that Ignite instance is started after all other
 * Spring beans have been initialized and right before Spring context is refreshed.
 * That implies that it's not valid to reference IgniteSpringBean from
 * any kind of Spring bean init methods like {@link javax.annotation.PostConstruct}.
 * If it's required to reference IgniteSpringBean for other bean
 * initialization purposes, it should be done from a {@link ContextRefreshedEvent}
 * listener method declared in that bean.
 * </p>
 *
 * <p>
 * <h1 class="header">Spring Configuration Example</h1>
 * Here is a typical example of describing it in Spring file:
 * <pre class="xml">
 * &lt;bean id="mySpringBean" class="org.apache.ignite.IgniteSpringBean"&gt;
 *     &lt;property name="configuration"&gt;
 *         &lt;bean id="grid.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *             &lt;property name="igniteInstanceName" value="mySpringGrid"/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 * Or use default configuration:
 * <pre class="xml">
 * &lt;bean id="mySpringBean" class="org.apache.ignite.IgniteSpringBean"/&gt;
 * </pre>
 * <h1 class="header">Java Example</h1>
 * Here is how you may access this bean from code:
 * <pre class="java">
 * AbstractApplicationContext ctx = new FileSystemXmlApplicationContext("/path/to/spring/file");
 *
 * // Register Spring hook to destroy bean automatically.
 * ctx.registerShutdownHook();
 *
 * Ignite ignite = (Ignite)ctx.getBean("mySpringBean");
 * </pre>
 * </p>
 */
public class DeferredStartIgniteSpringBean implements Ignite, DisposableBean, SmartInitializingSingleton,
        ApplicationContextAware, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Ignite g;

    /** */
    private IgniteConfiguration cfg;

    /** */
    private ApplicationContext appCtx;

    public void start() throws IgniteCheckedException {
        if (cfg == null) {
            cfg = new IgniteConfiguration();
        }

        g = IgniteSpring.start(cfg, appCtx);
    }

    /** {@inheritDoc} */
     public IgniteConfiguration configuration() {
        return cfg;
    }

    /**
     * Gets the configuration of this Ignite instance.
     * <p>
     * This method is required for proper Spring integration and is the same as
     * {@link #configuration()}.
     * See https://issues.apache.org/jira/browse/IGNITE-1102 for details.
     * <p>
     * <b>NOTE:</b>
     * <br>
     * SPIs obtains through this method should never be used directly. SPIs provide
     * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
     * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
     * via this method to check its configuration properties or call other non-SPI
     * methods.
     *
     * @return Ignite configuration instance.
     * @see #configuration()
     */
    public IgniteConfiguration getConfiguration() {
        return cfg;
    }

    /**
     * Sets Ignite configuration.
     *
     * @param cfg Ignite configuration.
     */
    public void setConfiguration(IgniteConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Gets the spring application context this Ignite runs in.
     *
     * @return Application context this Ignite runs in.
     */
    public ApplicationContext getApplicationContext() throws BeansException {
        return appCtx;
    }

    /** {@inheritDoc} */
     public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        appCtx = ctx;
    }

    /** {@inheritDoc} */
     public void destroy() throws Exception {
        if (g != null) {
            // Do not cancel started tasks, wait for them.
            G.stop(g.name(), false);
        }
    }

    /** {@inheritDoc} */
     public void afterSingletonsInstantiated() {
        if (cfg == null)
            cfg = new IgniteConfiguration();

        try {
            g = IgniteSpring.start(cfg, appCtx);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to start IgniteSpringBean", e);
        }
    }

    /** {@inheritDoc} */
     public IgniteLogger log() {
        checkIgnite();

        return cfg.getGridLogger();
    }

    /** {@inheritDoc} */
     public IgniteProductVersion version() {
        checkIgnite();

        return g.version();
    }

    /** {@inheritDoc} */
     public IgniteCompute compute() {
        checkIgnite();

        return g.compute();
    }

    /** {@inheritDoc} */
     public IgniteServices services() {
        checkIgnite();

        return g.services();
    }

    /** {@inheritDoc} */
     public IgniteMessaging message() {
        checkIgnite();

        return g.message();
    }

    /** {@inheritDoc} */
     public IgniteEvents events() {
        checkIgnite();

        return g.events();
    }

    /** {@inheritDoc} */
     public ExecutorService executorService() {
        checkIgnite();

        return g.executorService();
    }

    /** {@inheritDoc} */
     public IgniteCluster cluster() {
        checkIgnite();

        return g.cluster();
    }

    /** {@inheritDoc} */
     public IgniteCompute compute(ClusterGroup grp) {
        checkIgnite();

        return g.compute(grp);
    }

    /** {@inheritDoc} */
     public IgniteMessaging message(ClusterGroup prj) {
        checkIgnite();

        return g.message(prj);
    }

    /** {@inheritDoc} */
     public IgniteEvents events(ClusterGroup grp) {
        checkIgnite();

        return g.events(grp);
    }

    /** {@inheritDoc} */
     public IgniteServices services(ClusterGroup grp) {
        checkIgnite();

        return g.services(grp);
    }

    /** {@inheritDoc} */
     public ExecutorService executorService(ClusterGroup grp) {
        checkIgnite();

        return g.executorService(grp);
    }

    /** {@inheritDoc} */
     public IgniteScheduler scheduler() {
        checkIgnite();

        return g.scheduler();
    }

    /** {@inheritDoc} */
     public String name() {
        checkIgnite();

        return g.name();
    }

    /** {@inheritDoc} */
     public void resetLostPartitions(Collection<String> cacheNames) {
        checkIgnite();

        g.resetLostPartitions(cacheNames);
    }

    /** {@inheritDoc} */
     public Collection<DataRegionMetrics> dataRegionMetrics() {
        checkIgnite();

        return g.dataRegionMetrics();
    }

    /** {@inheritDoc} */
    @Nullable  public DataRegionMetrics dataRegionMetrics(String memPlcName) {
        checkIgnite();

        return g.dataRegionMetrics(memPlcName);
    }

    /** {@inheritDoc} */
     public DataStorageMetrics dataStorageMetrics() {
        checkIgnite();

        return g.dataStorageMetrics();
    }

    /** {@inheritDoc} */
     public Collection<MemoryMetrics> memoryMetrics() {
        return DataRegionMetricsAdapter.collectionOf(dataRegionMetrics());
    }

    /** {@inheritDoc} */
    @Nullable  public MemoryMetrics memoryMetrics(String memPlcName) {
        return DataRegionMetricsAdapter.valueOf(dataRegionMetrics(memPlcName));
    }

    /** {@inheritDoc} */
     public PersistenceMetrics persistentStoreMetrics() {
        return DataStorageMetricsAdapter.valueOf(dataStorageMetrics());
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> cache(@Nullable String name) {
        checkIgnite();

        return g.cache(name);
    }


    /** {@inheritDoc} */
     public Collection<String> cacheNames() {
        checkIgnite();

        return g.cacheNames();
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        checkIgnite();

        return g.createCache(cacheCfg);
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        checkIgnite();

        return g.getOrCreateCache(cacheCfg);
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
                                                 NearCacheConfiguration<K, V> nearCfg) {
        checkIgnite();

        return g.createCache(cacheCfg, nearCfg);
    }

    /** {@inheritDoc} */
     public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> cacheCfgs) {
        checkIgnite();

        return g.createCaches(cacheCfgs);
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg, NearCacheConfiguration<K, V> nearCfg) {
        checkIgnite();

        return g.getOrCreateCache(cacheCfg, nearCfg);
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> createNearCache(String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        checkIgnite();

        return g.createNearCache(cacheName, nearCfg);
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        checkIgnite();

        return g.getOrCreateNearCache(cacheName, nearCfg);
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        checkIgnite();

        return g.getOrCreateCache(cacheName);
    }

    /** {@inheritDoc} */
     public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> cacheCfgs) {
        checkIgnite();

        return g.getOrCreateCaches(cacheCfgs);
    }

    /** {@inheritDoc} */
     public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        checkIgnite();

        return g.createCache(cacheName);
    }

    /** {@inheritDoc} */
     public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        checkIgnite();

        g.addCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
     public void destroyCache(String cacheName) {
        checkIgnite();

        g.destroyCache(cacheName);
    }

    /** {@inheritDoc} */
     public void destroyCaches(Collection<String> cacheNames) {
        checkIgnite();

        g.destroyCaches(cacheNames);
    }

    /** {@inheritDoc} */
     public IgniteTransactions transactions() {
        checkIgnite();

        return g.transactions();
    }

    /** {@inheritDoc} */
     public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        checkIgnite();

        return g.dataStreamer(cacheName);
    }

    /** {@inheritDoc} */
     public IgniteFileSystem fileSystem(String name) {
        checkIgnite();

        return g.fileSystem(name);
    }

    /** {@inheritDoc} */
     public Collection<IgniteFileSystem> fileSystems() {
        checkIgnite();

        return g.fileSystems();
    }

    /** {@inheritDoc} */
     public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        checkIgnite();

        return g.plugin(name);
    }

    /** {@inheritDoc} */
     public IgniteBinary binary() {
        checkIgnite();

        return g.binary();
    }

    /** {@inheritDoc} */
     public void close() throws IgniteException {
        g.close();
    }

    /** {@inheritDoc} */
    @Nullable  public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) {
        checkIgnite();

        return g.atomicSequence(name, initVal, create);
    }

    /** {@inheritDoc} */
     public IgniteAtomicSequence atomicSequence(String name, AtomicConfiguration cfg, long initVal,
                                                boolean create) throws IgniteException {
        checkIgnite();

        return g.atomicSequence(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable  public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) {
        checkIgnite();

        return g.atomicLong(name, initVal, create);
    }

     public IgniteAtomicLong atomicLong(String name, AtomicConfiguration cfg, long initVal,
                                        boolean create) throws IgniteException {
        checkIgnite();

        return g.atomicLong(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable  public <T> IgniteAtomicReference<T> atomicReference(String name,
                                                                   @Nullable T initVal,
                                                                   boolean create)
    {
        checkIgnite();

        return g.atomicReference(name, initVal, create);
    }

    /** {@inheritDoc} */
     public <T> IgniteAtomicReference<T> atomicReference(String name, AtomicConfiguration cfg,
                                                         @Nullable T initVal, boolean create) throws IgniteException {
        checkIgnite();

        return g.atomicReference(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable  public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
                                                                     @Nullable T initVal,
                                                                     @Nullable S initStamp,
                                                                     boolean create)
    {
        checkIgnite();

        return g.atomicStamped(name, initVal, initStamp, create);
    }

     public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, AtomicConfiguration cfg,
                                                           @Nullable T initVal, @Nullable S initStamp, boolean create) throws IgniteException {
        checkIgnite();

        return g.atomicStamped(name, cfg, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Nullable  public IgniteCountDownLatch countDownLatch(String name,
                                                          int cnt,
                                                          boolean autoDel,
                                                          boolean create)
    {
        checkIgnite();

        return g.countDownLatch(name, cnt, autoDel, create);
    }

    /** {@inheritDoc} */
    @Nullable  public IgniteSemaphore semaphore(String name,
                                                int cnt,
                                                boolean failoverSafe,
                                                boolean create)
    {
        checkIgnite();

        return g.semaphore(name, cnt,
                failoverSafe, create);
    }

    /** {@inheritDoc} */
    @Nullable  public IgniteLock reentrantLock(String name,
                                               boolean failoverSafe,
                                               boolean fair,
                                               boolean create)
    {
        checkIgnite();

        return g.reentrantLock(name, failoverSafe, fair, create);
    }

    /** {@inheritDoc} */
    @Nullable  public <T> IgniteQueue<T> queue(String name,
                                               int cap,
                                               CollectionConfiguration cfg)
    {
        checkIgnite();

        return g.queue(name, cap, cfg);
    }

    /** {@inheritDoc} */
    @Nullable  public <T> IgniteSet<T> set(String name,
                                           CollectionConfiguration cfg)
    {
        checkIgnite();

        return g.set(name, cfg);
    }

    /** {@inheritDoc} */
     public <K> Affinity<K> affinity(String cacheName) {
        return g.affinity(cacheName);
    }

    /** {@inheritDoc} */
     public boolean active() {
        checkIgnite();

        return g.active();
    }

    /** {@inheritDoc} */
     public void active(boolean active) {
        checkIgnite();

        g.active(active);
    }

    /** {@inheritDoc} */
     public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(g);
    }

    /** {@inheritDoc} */
     public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        g = (Ignite)in.readObject();

        cfg = g.configuration();
    }

    /**
     * Checks if this bean is valid.
     *
     * @throws IllegalStateException If bean is not valid, i.e. Ignite has already been stopped
     *      or has not yet been started.
     */
    protected void checkIgnite() throws IllegalStateException {
        if (g == null) {
            throw new IllegalStateException("Ignite is in invalid state to perform this operation. " +
                    "It either not started yet or has already being or have stopped.\n" +
                    "Make sure that IgniteSpringBean is not referenced from any kind of Spring bean init methods " +
                    "like @PostConstruct}.\n" +
                    "[ignite=" + g + ", cfg=" + cfg + ']');
        }
    }

    /** {@inheritDoc} */
     public String toString() {
        return S.toString(DeferredStartIgniteSpringBean.class, this);
    }
}