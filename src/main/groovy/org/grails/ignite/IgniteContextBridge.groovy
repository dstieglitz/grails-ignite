package org.grails.ignite

import org.apache.ignite.*
import org.apache.ignite.cache.affinity.Affinity
import org.apache.ignite.cluster.ClusterGroup
import org.apache.ignite.configuration.*
import org.apache.ignite.lang.IgniteProductVersion
import org.apache.ignite.plugin.IgnitePlugin
import org.apache.ignite.plugin.PluginNotFoundException
import org.jetbrains.annotations.Nullable

import javax.cache.CacheException
import java.util.concurrent.ExecutorService

/**
 * A Spring bean for the Ignite grid that can be configured in the main Grails application context, but bridges to
 * a separate application context, which is where the Ignite grid is actually configured.
 */
class IgniteContextBridge implements Ignite {

    private Ignite underlyingIgnite

    private Ignite getOrCreateIgnite() {
        if (underlyingIgnite == null) {
            if (IgniteStartupHelper.grid == null) {
                IgniteStartupHelper.startIgnite()
            }
            underlyingIgnite = IgniteStartupHelper.grid
        }

        return underlyingIgnite
    }

    @Override
    String name() {
        return getOrCreateIgnite().name()
    }

    @Override
    IgniteLogger log() {
        return getOrCreateIgnite().log()
    }

    @Override
    IgniteConfiguration configuration() {
        return getOrCreateIgnite().configuration()
    }

    @Override
    IgniteCluster cluster() {
        return getOrCreateIgnite().cluster()
    }

    @Override
    IgniteCompute compute() {
        return getOrCreateIgnite().compute()
    }

    @Override
    IgniteCompute compute(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().compute(clusterGroup)
    }

    @Override
    IgniteMessaging message() {
        return getOrCreateIgnite().message()
    }

    @Override
    IgniteMessaging message(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().message(clusterGroup)
    }

    @Override
    IgniteEvents events() {
        return getOrCreateIgnite().events()
    }

    @Override
    IgniteEvents events(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().events(clusterGroup)
    }

    @Override
    IgniteServices services() {
        return getOrCreateIgnite().services()
    }

    @Override
    IgniteServices services(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().services(clusterGroup)
    }

    @Override
    ExecutorService executorService() {
        return getOrCreateIgnite().executorService()
    }

    @Override
    ExecutorService executorService(ClusterGroup clusterGroup) {
        return getOrCreateIgnite().executorService(clusterGroup)
    }

    @Override
    IgniteProductVersion version() {
        return getOrCreateIgnite().version()
    }

    @Override
    IgniteScheduler scheduler() {
        return getOrCreateIgnite().scheduler()
    }

    @Override
    <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> kvCacheConfiguration) {
        return getOrCreateIgnite().createCache(kvCacheConfiguration)
    }

    @Override
    Collection<IgniteCache> createCaches(Collection<CacheConfiguration> collection) throws CacheException {
        return getOrCreateIgnite().createCaches(collection)
    }

    @Override
    <K, V> IgniteCache<K, V> createCache(String s) {
        return getOrCreateIgnite().createCache(s)
    }

    @Override
    <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> kvCacheConfiguration) {
        return getOrCreateIgnite().getOrCreateCache(kvCacheConfiguration)
    }

    @Override
    <K, V> IgniteCache<K, V> getOrCreateCache(String s) {
        return getOrCreateIgnite().getOrCreateCache(s)
    }

    @Override
    Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> collection) throws CacheException {
        return getOrCreateIgnite().getOrCreateCaches(collection)
    }

    @Override
    <K, V> void addCacheConfiguration(CacheConfiguration<K, V> kvCacheConfiguration) {
        getOrCreateIgnite().addCacheConfiguration(kvCacheConfiguration)
    }

    @Override
    <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> kvCacheConfiguration, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return getOrCreateIgnite().createCache(kvCacheConfiguration, kvNearCacheConfiguration)
    }

    @Override
    <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> kvCacheConfiguration, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return getOrCreateIgnite().getOrCreateCache(kvCacheConfiguration, kvNearCacheConfiguration)
    }

    @Override
    <K, V> IgniteCache<K, V> createNearCache(@Nullable String s, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return getOrCreateIgnite().createNearCache(s, kvNearCacheConfiguration)
    }

    @Override
    <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String s, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return getOrCreateIgnite().getOrCreateNearCache(s, kvNearCacheConfiguration)
    }

    @Override
    void destroyCache(String s) {
        getOrCreateIgnite().destroyCache(s)
    }

    @Override
    void destroyCaches(Collection<String> collection) throws CacheException {
        getOrCreateIgnite().destroyCaches(collection)
    }

    @Override
    <K, V> IgniteCache<K, V> cache(@Nullable String s) {
        return getOrCreateIgnite().cache(s)
    }

    @Override
    Collection<String> cacheNames() {
        return getOrCreateIgnite().cacheNames()
    }

    @Override
    IgniteTransactions transactions() {
        return getOrCreateIgnite().transactions()
    }

    @Override
    <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String s) {
        return getOrCreateIgnite().dataStreamer(s)
    }

    @Override
    IgniteFileSystem fileSystem(String s) {
        return getOrCreateIgnite().fileSystem(s)
    }

    @Override
    Collection<IgniteFileSystem> fileSystems() {
        return getOrCreateIgnite().fileSystems()
    }

    @Override
    IgniteAtomicSequence atomicSequence(String s, long l, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicSequence(s, l, b)
    }

    @Override
    IgniteAtomicSequence atomicSequence(String s, AtomicConfiguration atomicConfiguration, long l, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicSequence(s, atomicConfiguration, l, b)
    }

    @Override
    IgniteAtomicLong atomicLong(String s, long l, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicLong(s, l, b)
    }

    @Override
    IgniteAtomicLong atomicLong(String s, AtomicConfiguration atomicConfiguration, long l, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicLong(s, atomicConfiguration, l, b)
    }

    @Override
    <T> IgniteAtomicReference<T> atomicReference(String s, @Nullable T t, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicReference(s, t, b)
    }

    @Override
    <T> IgniteAtomicReference<T> atomicReference(String s, AtomicConfiguration atomicConfiguration, @Nullable T t, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicReference(s, atomicConfiguration, t, b)
    }

    @Override
    <T, S> IgniteAtomicStamped<T, S> atomicStamped(String s, @Nullable T t, @Nullable S s2, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicStamped(s, t, s2, b)
    }

    @Override
    <T, S> IgniteAtomicStamped<T, S> atomicStamped(String s, AtomicConfiguration atomicConfiguration, @Nullable T t, @Nullable S s1, boolean b) throws IgniteException {
        return getOrCreateIgnite().atomicStamped(s, atomicConfiguration, t, s1, b)
    }

    @Override
    IgniteCountDownLatch countDownLatch(String s, int i, boolean b, boolean b2) throws IgniteException {
        return getOrCreateIgnite().countDownLatch(s, i, b, b2)
    }

    @Override
    IgniteSemaphore semaphore(String s, int i, boolean b, boolean b1) throws IgniteException {
        return getOrCreateIgnite().semaphore(s, i, b, b1)
    }

    @Override
    IgniteLock reentrantLock(String s, boolean b, boolean b1, boolean b2) throws IgniteException {
        return getOrCreateIgnite().reentrantLock(s, b, b1, b2)
    }

    @Override
    <T> IgniteQueue<T> queue(String s, int i, @Nullable CollectionConfiguration collectionConfiguration) throws IgniteException {
        return getOrCreateIgnite().queue(s, i, collectionConfiguration)
    }

    @Override
    <T> IgniteSet<T> set(String s, @Nullable CollectionConfiguration collectionConfiguration) throws IgniteException {
        return getOrCreateIgnite().set(s, collectionConfiguration)
    }

    @Override
    <T extends IgnitePlugin> T plugin(String s) throws PluginNotFoundException {
        return getOrCreateIgnite().plugin(s)
    }

    @Override
    IgniteBinary binary() {
        return getOrCreateIgnite().binary()
    }

    @Override
    void close() throws IgniteException {
        getOrCreateIgnite().close()
    }

    @Override
    <K> Affinity<K> affinity(String s) {
        return getOrCreateIgnite().affinity(s)
    }

    @Override
    boolean active() {
        return getOrCreateIgnite().active()
    }

    @Override
    void active(boolean b) {
        getOrCreateIgnite().active(b)
    }

    @Override
    void resetLostPartitions(Collection<String> collection) {
        getOrCreateIgnite().resetLostPartitions(collection)
    }

    @Override
    Collection<MemoryMetrics> memoryMetrics() {
        return getOrCreateIgnite().memoryMetrics()
    }

    @Override
    MemoryMetrics memoryMetrics(String s) {
        return null
    }

    @Override
    PersistenceMetrics persistentStoreMetrics() {
        return null
    }

    @Override
    Collection<DataRegionMetrics> dataRegionMetrics() {
        return getOrCreateIgnite().dataRegionMetrics()
    }

    @Override
    DataRegionMetrics dataRegionMetrics(String s) {
        return getOrCreateIgnite().dataRegionMetrics(s)
    }

    @Override
    DataStorageMetrics dataStorageMetrics() {
        return getOrCreateIgnite().dataStorageMetrics()
    }
}
