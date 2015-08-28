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
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * Created with IntelliJ IDEA.
 * User: dstieglitz
 * Date: 8/28/15
 * Time: 8:25 AM
 * To change this template use File | Settings | File Templates.
 */
public class IgniteContextBridge implements Ignite {

    private Ignite underlyingIgnite;

    public IgniteContextBridge() {
        underlyingIgnite = IgniteStartupHelper.grid;
    }

    @Override
    public String name() {
        return underlyingIgnite.name();
    }

    @Override
    public IgniteLogger log() {
        return underlyingIgnite.log();
    }

    @Override
    public IgniteConfiguration configuration() {
        return underlyingIgnite.configuration();
    }

    @Override
    public IgniteCluster cluster() {
        return underlyingIgnite.cluster();
    }

    @Override
    public IgniteCompute compute() {
        return underlyingIgnite.compute();
    }

    @Override
    public IgniteCompute compute(ClusterGroup clusterGroup) {
        return underlyingIgnite.compute(clusterGroup);
    }

    @Override
    public IgniteMessaging message() {
        return underlyingIgnite.message();
    }

    @Override
    public IgniteMessaging message(ClusterGroup clusterGroup) {
        return underlyingIgnite.message(clusterGroup);
    }

    @Override
    public IgniteEvents events() {
        return underlyingIgnite.events();
    }

    @Override
    public IgniteEvents events(ClusterGroup clusterGroup) {
        return underlyingIgnite.events(clusterGroup);
    }

    @Override
    public IgniteServices services() {
        return underlyingIgnite.services();
    }

    @Override
    public IgniteServices services(ClusterGroup clusterGroup) {
        return underlyingIgnite.services(clusterGroup);
    }

    @Override
    public ExecutorService executorService() {
        return underlyingIgnite.executorService();
    }

    @Override
    public ExecutorService executorService(ClusterGroup clusterGroup) {
        return underlyingIgnite.executorService(clusterGroup);
    }

    @Override
    public IgniteProductVersion version() {
        return underlyingIgnite.version();
    }

    @Override
    public IgniteScheduler scheduler() {
        return underlyingIgnite.scheduler();
    }

    @Override
    public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> kvCacheConfiguration) {
        return underlyingIgnite.createCache(kvCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> createCache(String s) {
        return underlyingIgnite.createCache(s);
    }

    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> kvCacheConfiguration) {
        return underlyingIgnite.getOrCreateCache(kvCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(String s) {
        return underlyingIgnite.getOrCreateCache(s);
    }

    @Override
    public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> kvCacheConfiguration) {
        underlyingIgnite.addCacheConfiguration(kvCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> kvCacheConfiguration, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return underlyingIgnite.createCache(kvCacheConfiguration, kvNearCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> kvCacheConfiguration, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return underlyingIgnite.getOrCreateCache(kvCacheConfiguration, kvNearCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> createNearCache(@Nullable String s, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return underlyingIgnite.createNearCache(s, kvNearCacheConfiguration);
    }

    @Override
    public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String s, NearCacheConfiguration<K, V> kvNearCacheConfiguration) {
        return underlyingIgnite.getOrCreateNearCache(s, kvNearCacheConfiguration);
    }

    @Override
    public void destroyCache(String s) {
        underlyingIgnite.destroyCache(s);
    }

    @Override
    public <K, V> IgniteCache<K, V> cache(@Nullable String s) {
        return underlyingIgnite.cache(s);
    }

    @Override
    public IgniteTransactions transactions() {
        return underlyingIgnite.transactions();
    }

    @Override
    public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String s) {
        return underlyingIgnite.dataStreamer(s);
    }

    @Override
    public IgniteFileSystem fileSystem(String s) {
        return underlyingIgnite.fileSystem(s);
    }

    @Override
    public Collection<IgniteFileSystem> fileSystems() {
        return underlyingIgnite.fileSystems();
    }

    @Override
    public IgniteAtomicSequence atomicSequence(String s, long l, boolean b) throws IgniteException {
        return underlyingIgnite.atomicSequence(s, l, b);
    }

    @Override
    public IgniteAtomicLong atomicLong(String s, long l, boolean b) throws IgniteException {
        return underlyingIgnite.atomicLong(s, l, b);
    }

    @Override
    public <T> IgniteAtomicReference<T> atomicReference(String s, @Nullable T t, boolean b) throws IgniteException {
        return underlyingIgnite.atomicReference(s, t, b);
    }

    @Override
    public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String s, @Nullable T t, @Nullable S s2, boolean b) throws IgniteException {
        return underlyingIgnite.atomicStamped(s, t, s2, b);
    }

    @Override
    public IgniteCountDownLatch countDownLatch(String s, int i, boolean b, boolean b2) throws IgniteException {
        return underlyingIgnite.countDownLatch(s, i, b, b2);
    }

    @Override
    public <T> IgniteQueue<T> queue(String s, int i, @Nullable CollectionConfiguration collectionConfiguration) throws IgniteException {
        return underlyingIgnite.queue(s, i, collectionConfiguration);
    }

    @Override
    public <T> IgniteSet<T> set(String s, @Nullable CollectionConfiguration collectionConfiguration) throws IgniteException {
        return underlyingIgnite.set(s, collectionConfiguration);
    }

    @Override
    public <T extends IgnitePlugin> T plugin(String s) throws PluginNotFoundException {
        return underlyingIgnite.plugin(s);
    }

    @Override
    public void close() throws IgniteException {
        underlyingIgnite.close();
    }

    @Override
    public <K> Affinity<K> affinity(String s) {
        return underlyingIgnite.affinity(s);
    }
}
