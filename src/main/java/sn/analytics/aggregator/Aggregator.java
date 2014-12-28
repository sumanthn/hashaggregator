package sn.analytics.aggregator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.hash.Hasher;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.joda.time.DateTime;
import sn.analytics.GlobalRepo;
import sn.analytics.type.*;
import sn.analytics.util.GlobalIdGenerator;
import sn.analytics.util.KryoPool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.util.Set;

/**
 * Created by Sumanth on 28/12/14.
 */
public class Aggregator extends AbstractAggregator implements IAggregator {

    private final AggregateType aggregateType;
    private TimeGranularity timeGranularity = TimeGranularity.HOUR_OF_DAY;
    CacheStore cacheStore = CacheStore.getInstance();
    private String keyCacheId = "";
    private String dataCacheId = "";

    public Aggregator(final AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    public Aggregator(AggregateType aggregateType, TimeGranularity timeGranularity) {
        this.aggregateType = aggregateType;
        this.timeGranularity = timeGranularity;
    }

    public AggregateType getAggregateType() {
        return aggregateType;
    }

    public TimeGranularity getTimeGranularity() {
        return timeGranularity;
    }

    public void setTimeGranularity(TimeGranularity timeGranularity) {
        this.timeGranularity = timeGranularity;
    }

    @Override
    public void init() {
        keyCacheId = "K_" + GlobalIdGenerator.getInstance().getId();
        keyCache = cacheStore.makeKeyCache(keyCacheId);
        dataCacheId = "D_" + GlobalIdGenerator.getInstance().getId();
        dataCache = cacheStore.makeDataCache(dataCacheId);
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
            hasher.putLong(timeDimensionVal);
            groupByKey.append(ts.toString(MINUTES_FORMAT) + ":" + "00");

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
                dateTimeCache.put(timeDimensionVal, ts.toString(SECONDS_FORMAT));
            }

            MetricData metricData = null;
            if (!isNewKey) {
                metricData = dataCache.get(hashKey);
            } else {
                metricData = new MetricData(GlobalRepo.getInstance().getMetrics(aggregateType));
            }

            for (String metricName : GlobalRepo.getInstance().getMetrics(aggregateType)) {
                try {
                    int val = Integer.valueOf(metricPositions.get(metricName));
                    metricData.updateDataPoint(metricName, val);
                } catch (Exception e) {

                }
            }

            metricData.updateCount();
            //putting back mutuated data :
            dataCache.put(hashKey, metricData);

        }//fi ts

    }

    @Override
    public void postProcess() {
        System.out.println("Dump key cache");
        Set<Long> keys = keyCache.keySet();
        for (Long key : keys) {
            System.out.print(key);
            GenericGroupByKey groupByKey = keyCache.get(key);
            String str = GenericGroupByKey.getStringFromBytes(groupByKey.getGroupByKey());
            System.out.println(": " + str);
            MetricData metricData = dataCache.get(key);
            if (metricData!=null){
                dumpMetricData(metricData);
                System.out.println("Count is:" +
                        metricData.getCount());

            }else{
                System.out.println("No metric data for " + key);
            }
        }
        keyCache.clear();
        dataCache.clear();
        cacheStore.deleteCache(keyCacheId);
        cacheStore.deleteCache(dataCacheId);

    }

    private void dumpMetricData(MetricData metricData) {
        for (String metricName : GlobalRepo.getInstance().getMetrics(aggregateType)) {

             MetricAggregate metricAgg =
                    metricData.getAggData(metricName);
            if (metricAgg!=null) {
                SummaryStatistics summaryStatistics = metricAgg.getSummaryStatistics();
                System.out.println(summaryStatistics.getN() + " " + summaryStatistics.getSum() + " " + summaryStatistics.getMin());
            }else{
                System.out.println("No data for metric " + metricName);
            }
        }

    }

    public static void main(String[] args) {

        GlobalRepo.getInstance().init();
        CacheStore.getInstance().init();
        final String fileName = "/tmp/factdata.csv";
        IAggregator aggregator = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            aggregator = new Aggregator(AggregateType.USER_AGENT, TimeGranularity.MINUTE_OF_DAY);
            aggregator.init();
            String header = reader.readLine();
            aggregator.processHeader(header);
            String line = reader.readLine();
            while (line != null) {

                String[] recordTokens = line.split(FLD_DELIM);
                aggregator.processRecord(recordTokens);
                aggregator.processRecord(recordTokens);
                line = reader.readLine();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        aggregator.postProcess();
        CacheStore.getInstance().close();
    }
}
