package sn.analytics.aggregator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.base.Joiner;
import com.google.common.hash.Hasher;
import org.infinispan.Cache;
import org.joda.time.DateTime;
import sn.analytics.GlobalRepo;
import sn.analytics.type.*;
import sn.analytics.util.GlobalIdGenerator;
import sn.analytics.util.KryoPool;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by Sumanth on 30/12/14.
 */
public class SessionActivityAggregator extends AbstractAggregator implements IAggregator {

    private AggregateType aggregateType = AggregateType.CLIENT_SESSION;
    private TimeGranularity timeGranularity = TimeGranularity.ALL;
    private final String outDir;
    CacheStore cacheStore = CacheStore.getInstance();
    private String keyCacheId = "";
    private String dataCacheId = "";

    private Cache<Long, CacheItem> dataCache;

    public SessionActivityAggregator(String outDir) {
        this.outDir = outDir;
    }

    @Override
    public void init() {
        keyCacheId = "K_" + GlobalIdGenerator.getInstance().getId();
        keyCache = cacheStore.makeKeyCache(keyCacheId);
        dataCacheId = "D_" + GlobalIdGenerator.getInstance().getId();
        dataCache = cacheStore.makeGenericDataCache(dataCacheId);
        aggregateType = AggregateType.CLIENT_SESSION;
    }

    @Override
    public void processHeader(String header) {
        discoverPositions(header, GlobalRepo.getInstance().getDimensions(aggregateType),
                GlobalRepo.getInstance().getMetrics(aggregateType));
    }

    @Override
    public void processRecord(String[] recordTokens) {
        //make key
        Hasher hasher = hashFunction.newHasher();
        StringBuilder groupByKey = new StringBuilder();

        final String tsStr = recordTokens[timeDimensionsPositions.get("EVENT_TIMESTAMP")];
        DateTime ts = strToDateTime(tsStr);
        if (ts != null) {
            //hasher.putInt();
            long timeDimensionVal = -1;
            //save on every bit of string
            switch (timeGranularity) {

                case MINUTE_OF_DAY:
                    timeDimensionVal = Long.valueOf(ts.toString(MINUTES_TRUNCATED_FORMAT));
                    break;
                case HOUR_OF_DAY:
                    timeDimensionVal = Long.valueOf(ts.toString(HOUR_TRUNCATED_FORMAT));

                    break;
                case DAY:
                    timeDimensionVal = Long.valueOf(ts.toString(DAY_TRUNCATED_FORMAT));
                    break;
                case ALL:
                    //use the same value
                    timeDimensionVal = 100;
                    break;
            }

            for (int i = 0; i < dimPositions.size(); i++) {
                //TODO: need a validation if they are indeed the dimensions
                //assume all to be strings
                groupByKey.append(recordTokens[dimPositions.get(i)]).append(FLD_DELIM);
                hasher.putString(recordTokens[dimPositions.get(i)], Charset.defaultCharset());

            }

            if (timeGranularity!=TimeGranularity.ALL) {
                hasher.putLong(timeDimensionVal);
                groupByKey.append(ts.toString(MINUTES_FORMAT) + ":" + "00");
            }

            boolean isNewKey = false;
            long hashKey = hasher.hash().asLong();
            if (!keyCache.containsKey(hashKey)) {
                Kryo kryo = KryoPool.getInstance().getKryo();

                ByteBufferOutput bufferOutput = new ByteBufferOutput(groupByKey.length() + 5);
                kryo.writeObject(bufferOutput, groupByKey.toString());

                GenericGroupByKey genericGroupByKey = new GenericGroupByKey(hashKey, bufferOutput.toBytes());
                keyCache.put(hashKey, genericGroupByKey);
                KryoPool.getInstance().returnToPool(kryo);
                bufferOutput.clear();
                isNewKey = true;
                // dateTimeCache.put(timeDimensionVal, ts.toString(SECONDS_FORMAT));
            }

            CacheItem cacheItem = null;
            if (!isNewKey) {
                cacheItem = dataCache.get(hashKey);
            } else {
                cacheItem = new CacheItem();
            }

            //get the URLs
            //only one for now
            for (String metricName : metricPositions.keySet()) {
                String accessUrl = recordTokens[metricPositions.get(metricName)];

                cacheItem.appendData(accessUrl);
            }

            processedRecord++;
            cacheItem.updateCount();
            //putting back mutuated data :
            dataCache.put(hashKey, cacheItem);

        }//fi ts

    }

    private String makeHeader() {
        StringBuilder header = new StringBuilder();
        Joiner.on(FLD_DELIM).appendTo(header, GlobalRepo.getInstance().getDimensions(aggregateType));
        if (timeGranularity!=TimeGranularity.ALL) {
            header.append(FLD_DELIM).append("timestamp");
        }
        //header.append(FLD_DELIM);

        for (String metricName : GlobalRepo.getInstance().getMetrics(aggregateType)) {
            header.append(metricName);
            header.append(FLD_DELIM);
        }

        //the record count for this aggregate
        header.append("Count");

        return header.toString();

    }

    @Override
    public String dumpAggregatedData() {

        String fileNamePrefix = null;
        switch (aggregateType) {

            case CLIENT_SESSION:
                fileNamePrefix = "Clientsession-Aggregates-";
                break;
        }
        Random rgen = new Random();
        final String filePath = outDir + "/" + fileNamePrefix + "-" + Math.abs(rgen.nextInt()) + ".csv";

        //dump the records
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(filePath));
            writer.write(makeHeader());
            writer.newLine();

            //dump the metrics
            Set<Long> keys = keyCache.keySet();
            for (Long key : keys) {
                //write the key
                GenericGroupByKey groupByKey = keyCache.get(key);
                String str = GenericGroupByKey.getStringFromBytes(groupByKey.getGroupByKey());
                writer.write(str);
                writer.write(FLD_DELIM);
                writeAggRecord(key, writer);
                writer.newLine();

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        if (writer != null)
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        return filePath;
    }

    private void writeAggRecord(final Long hashKey, final BufferedWriter writer) {

        CacheItem cacheItem = dataCache.get(hashKey);
        try {
            writer.write(cacheItem.getData());
            writer.write(",");
            writer.write("" + cacheItem.getCount());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void postProcess() {
        keyCache.clear();
        dataCache.clear();
        cacheStore.deleteCache(keyCacheId);
        cacheStore.deleteCache(dataCacheId);

    }

    public static void main(String[] args) {

        GlobalRepo.getInstance().init();
        CacheStore.getInstance().init();
        final String fileName = "/tmp/fact2.csv";
        List<IAggregator> aggregators = new ArrayList<IAggregator>();
        final String outDir = "/tmp/aggdata/";

        IAggregator userAgentAgg = new SessionActivityAggregator(outDir);

        aggregators.add(userAgentAgg);

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            for (IAggregator aggregator : aggregators)
                aggregator.init();
            String header = reader.readLine();

            for (IAggregator aggregator : aggregators)
                aggregator.processHeader(header);

            String line = reader.readLine();
            while (line != null) {

                String[] recordTokens = line.split(FLD_DELIM);

                for (IAggregator aggregator : aggregators)
                    aggregator.processRecord(recordTokens);

                line = reader.readLine();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        for (IAggregator aggregator : aggregators) {

            aggregator.dumpAggregatedData();
            aggregator.postProcess();
        }
        CacheStore.getInstance().close();

    }

}
