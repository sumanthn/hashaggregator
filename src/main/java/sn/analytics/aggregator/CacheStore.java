package sn.analytics.aggregator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import sn.analytics.type.CacheItem;
import sn.analytics.type.GenericGroupByKey;
import sn.analytics.type.MetricAggregate;
import sn.analytics.type.MetricData;
import sn.analytics.util.GlobalIdGenerator;
import sn.analytics.util.KryoPool;
import sn.analytics.util.UserAgentDataSet;

import java.nio.charset.Charset;
import java.util.Random;

/**
 * Cache Store for various aggregation
 * A disk based cache overflow what cannot be fit in memory
 * Created by Sumanth
 */
public class CacheStore {
    private static CacheStore ourInstance = new CacheStore();

    DefaultCacheManager manager;

    public static CacheStore getInstance() {
        return ourInstance;
    }

    private CacheStore() {
    }

    public synchronized void init() {

        ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();

        //ALERT CHANGING CONF HERE
//        cfgBuilder.persistence().passivation(true).addSingleFileStore().purgeOnStartup(true).location("/tmp/cachefiles/").create();
        cfgBuilder.eviction().strategy(EvictionStrategy.LRU).maxEntries(10)
                .persistence()
                .passivation(false).addSingleFileStore()
                .fetchPersistentState(true)
                .ignoreModifications(false)
                .purgeOnStartup(true).
                location("/tmp/cachefiles/").
                create();

        //cfgBuilder.eviction().create();

        Configuration configuration = cfgBuilder.build();
         manager = new DefaultCacheManager(configuration);
        manager.defineConfiguration("Cache1",configuration);


    }

    public void close() {
        manager.stop();
    }

    public synchronized Cache<Long,GenericGroupByKey> makeKeyCache(final String name){
        return manager.getCache(name,true);
    }

    public synchronized  Cache<Long,MetricData> makeDataCache(final String name){
        return manager.getCache(name,true);
    }

    
    public synchronized Cache<Long,CacheItem> makeGenericDataCache(final String name) { return manager.getCache(name,true);}
    
    public synchronized  void deleteCache(final String name){
        manager.removeCache(name);
    }
    public static void main(String[] args) {

        CacheStore.getInstance().init();
         HashFunction hf   = Hashing.murmur3_128(new Random().nextInt());;

        CacheStore store = CacheStore.getInstance();
        String keyCacheId = "Key-"+GlobalIdGenerator.getInstance().getId();
        Cache<Long,GenericGroupByKey> keyCache = store.makeKeyCache(keyCacheId);
        Kryo kryo = KryoPool.getInstance().getKryo();

        int minOfDay =0;

        Random random = new Random();
        long recordCount =0;
            for(int i =0;i < 10;i++){
                minOfDay = random.nextInt(1020202);
            for(String userAgentStr : UserAgentDataSet.userAgentSet) {

                Hasher hasher = hf.newHasher().putInt(minOfDay);
                final String completeStr = userAgentStr + "," + minOfDay;
                hasher.putString(completeStr, Charset.defaultCharset());

                ByteBufferOutput output = new ByteBufferOutput(completeStr.length() + 5);
                kryo.writeObject(output, completeStr);
                final long hashKey = hasher.hash().asLong();
                GenericGroupByKey groupByKey = new GenericGroupByKey(hashKey, output.toBytes());
                keyCache.put(hashKey, groupByKey);
                output.clear();

                recordCount++;

            }

        }

       store.deleteCache(keyCacheId);

        System.out.println("completed record " + recordCount);


        store.close();

    }
}
