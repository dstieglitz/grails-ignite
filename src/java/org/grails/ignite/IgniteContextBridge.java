package org.grails.ignite;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.jetbrains.annotations.Nullable;

import javax.cache.CacheException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * A Spring bean for the Ignite grid that can be configured in the main Grails application context, but bridges to
 * a separate application context, which is where the Ignite grid is actually configured.
 */
public class IgniteContextBridge implements Ignite {

    private Ignite underlyingIgnite;

    private Ignite getOrCreateIgnite() {
        if (underlyingIgnite == null) {
            if (IgniteStartupHelper.grid == null) {
                IgniteStartupHelper.startIgnite();
            }
            underlyingIgnite = IgniteStartupHelper.grid;
        }

        return underlyingIgnite;
    }

    @Override
    public String name() {
        return getOrCreateIgnite().name();
    }

    @Override
    public IgniteLogger log() {
        return getOrCreateIgnite().log();
    }

    @Override
    public IgniteConfiguration configuration() {
        return getOrCreateIgnite().configuration();
    }

    @Override
    public IgniteCluster cluster() {
        return getOrCreateIgnite().cluster();
    }

    @Override
    public IgniteCompute compute() {
        return getOrCreateIgnite().compute();
    }

    @Override
    public IgniteCompute compute(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().compute(clusterGroup);
    }

    @Override
    public IgniteMessaging message() {
        return getOrCreateIgnite().message();
    }

    @Override
    public IgniteMessaging message(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().message(clusterGroup);
    }

    @Override
    public IgniteEvents events() {
        return getOrCreateIgnite().events();
    }

    @Override
    public IgniteEvents events(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().events(clusterGroup);
    }

    @Override
    public IgniteServices services() {
        return getOrCreateIgnite().services();
    }

    @Override
    public IgniteServices services(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().services(clusterGroup);
    }

    @Override
    public ExecutorService executorService() {
        return getOrCreateIgnite().executorService();
    }

    @Override
    public ExecutorService executorService(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().executorService(clusterGroup);
    }

    @Override
    public IgniteProductVersion version() {
        return getOrCreateIgnite().version();
    }

    @Override
    public IgniteScheduler scheduler() {
        return getOrCreateIgnite().scheduler();
    }

    @Override
    public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> kvCacheConfiguration) {
        return getOrCreateIgnite().createCache(kvCacheConfiguration);
    }

    @Override
    public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> collection) throws CacheException {
        return getOrCreateIgnite().createCaches(collection);
    }

    @Override
    public <K, V> IgniteCache<K, V> createCache(String s) {
        return getOrCreateIgnite().createCache(s);
    }

    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> kvCacheConfiguration) {
        return getOrCreateIgnite().getOrCreateCache(kvCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(String s) {
        return getOrCreateIgnite().getOrCreateCache(s);
    }

    @Override
    public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> collection) throws CacheException {
        return getOrCreateIgnite().getOrCreateCaches(collection);
    }

    @Override
    public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> kvCacheConfiguration) {
        getOrCreateIgnite().addCacheConfiguration(kvCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> kvCacheConfiguration, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return getOrCreateIgnite().createCache(kvCacheConfiguration, kvNearCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> kvCacheConfiguration, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return getOrCreateIgnite().getOrCreateCache(kvCacheConfiguration, kvNearCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> createNearCache(@Nullable String s, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return getOrCreateIgnite().createNearCache(s, kvNearCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String s, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return getOrCreateIgnite().getOrCreateNearCache(s, kvNearCacheConfiguration);
    }

    @Override
    public void destroyCache(String s) {
        getOrCreateIgnite().destroyCache(s);
    }

    @Override
    public void destroyCaches(Collection<String> collection) throws CacheException {
        getOrCreateIgnite().destroyCaches(collection);
    }

    @Override
    public <K, V> IgniteCache<K, V> cache(@Nullable String s) {
        return getOrCreateIgnite().cache(s);
    }

    @Override
    public Collection<String> cacheNames() {
        return getOrCreateIgnite().cacheNames();
    }

    @Override
    public IgniteTransactions transactions() {
        return getOrCreateIgnite().transactions();
    }

    @Override
    public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String s) {
        return getOrCreateIgnite().dataStreamer(s);
    }

    @Override
    public IgniteFileSystem fileSystem(String s) {
        return getOrCreateIgnite().fileSystem(s);
    }

    @Override
    public Collection<IgniteFileSystem> fileSystems() {
        return getOrCreateIgnite().fileSystems();
    }

    @Override
    public IgniteAtomicSequence atomicSequence(String s, long l, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicSequence(s, l, b);
    }

    @Override
    public IgniteAtomicLong atomicLong(String s, long l, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicLong(s, l, b);
    }

    @Override
    public <T> IgniteAtomicReference<T> atomicReference(String s, @Nullable T t, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicReference(s, t, b);
    }

    @Override
    public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String s, @Nullable T t, @Nullable S s2, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicStamped(s, t, s2, b);
    }

    @Override
    public IgniteCountDownLatch countDownLatch(String s, int i, boolean b, boolean b2) throws IgniteException {
        return getOrCreateIgnite().countDownLatch(s, i, b, b2);
    }

    @Override
    public IgniteSemaphore semaphore(String s, int i, boolean b, boolean b1) throws IgniteException {
        return getOrCreateIgnite().semaphore(s, i, b, b1);
    }

    @Override
    public IgniteLock reentrantLock(String s, boolean b, boolean b1, boolean b2) throws IgniteException {
        return getOrCreateIgnite().reentrantLock(s, b, b1, b2);
    }

    @Override
    public <T> IgniteQueue<T> queue(String s, int i, @Nullable CollectionConfiguration collectionConfiguration) throws IgniteException {
        return getOrCreateIgnite().queue(s, i, collectionConfiguration);
    }

    @Override
    public <T> IgniteSet<T> set(String s, @Nullable CollectionConfiguration collectionConfiguration) throws IgniteException {
        return getOrCreateIgnite().set(s, collectionConfiguration);
    }

    @Override
    public <T extends IgnitePlugin> T plugin(String s) throws PluginNotFoundException {
        return getOrCreateIgnite().plugin(s);
    }

    @Override
    public IgniteBinary binary() {
        return getOrCreateIgnite().binary();
    }

    @Override
    public void close() throws IgniteException {
        getOrCreateIgnite().close();
    }

    @Override
    public <K> Affinity<K> affinity(String s) {
        return getOrCreateIgnite().affinity(s);
    }

    @Override
    public boolean active() {
        return getOrCreateIgnite().active();
    }

    @Override
    public void active(boolean b) {
        getOrCreateIgnite().active(b);
    }

    @Override
    public void resetLostPartitions(Collection<String> collection) {
        getOrCreateIgnite().resetLostPartitions(collection);
    }

    @Override
    public Collection<MemoryMetrics> memoryMetrics() {
        return getOrCreateIgnite().memoryMetrics();
    }
}
