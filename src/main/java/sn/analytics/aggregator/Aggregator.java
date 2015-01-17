package sn.analytics.aggregator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.base.Joiner;
import com.google.common.hash.Hasher;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.infinispan.Cache;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sn.analytics.GlobalRepo;
import sn.analytics.type.*;
import sn.analytics.util.GlobalIdGenerator;
import sn.analytics.util.KryoPool;

import java.io.*;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by Sumanth on 28/12/14.
 */
public class Aggregator extends AbstractAggregator implements IAggregator {
    private static final Logger logger = LoggerFactory.getLogger(AbstractAggregator.class);

    private final AggregateType aggregateType;

    private TimeGranularity timeGranularity = TimeGranularity.HOUR_OF_DAY;
    private final String outDir;
    CacheStore cacheStore = CacheStore.getInstance();
    private String keyCacheId = "";
    private String dataCacheId = "";
    protected Cache<Long,MetricData> dataCache;

    public Aggregator(final AggregateType aggregateType, String outDir) {
        this.aggregateType = aggregateType;
        this.outDir = outDir;
    }

    public Aggregator(AggregateType aggregateType, TimeGranularity timeGranularity, String outDir) {
        this.aggregateType = aggregateType;
        this.timeGranularity = timeGranularity;
        this.outDir = outDir;

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
               // dateTimeCache.put(timeDimensionVal, ts.toString(SECONDS_FORMAT));
            }

            MetricData metricData = null;
            if (!isNewKey) {
                metricData = dataCache.get(hashKey);
            } else {
                metricData = new MetricData(GlobalRepo.getInstance().getMetrics(aggregateType));
            }

            for (String metricName : GlobalRepo.getInstance().getMetrics(aggregateType)) {
                try {
                    int val = Integer.valueOf(recordTokens[metricPositions.get(metricName)]);

                    metricData.updateDataPoint(metricName, val);
                } catch (Exception e) {

                }
            }
            processedRecord++;
            metricData.updateCount();
            //putting back mutuated data :
            dataCache.put(hashKey, metricData);

        }//fi ts

    }

    @Override
    public String dumpAggregatedData() {

        String fileNamePrefix = null;
        switch (aggregateType) {

            case USER_AGENT:
                fileNamePrefix = "Useragent-Aggregates-";
                break;
            case GEO_LOCATION:
                fileNamePrefix = "Geolocation-Aggregates-";
                break;
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
        if (writer!=null)
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        
        logger.info("Aggregate data for aggregation type {} of time granularity {} in file {} ", aggregateType,timeGranularity,filePath);
        return filePath;
    }

    private String makeHeader() {
        StringBuilder header = new StringBuilder();
        Joiner.on(FLD_DELIM).appendTo(header, GlobalRepo.getInstance().getDimensions(aggregateType));
        header.append(FLD_DELIM).append("timestamp");
        header.append(FLD_DELIM);
        //sum,min,max,avg of each of the aggregates

        for (String metricName : GlobalRepo.getInstance().getMetrics(aggregateType)) {
            for (AggOperator aggOperator : AggOperator.values()) {
                header.append(metricName + "_" + aggOperator.toString());
                header.append(FLD_DELIM);
            }
        }

        //the record count for this aggregate
        header.append("Count");

        return header.toString();

    }
    static final DecimalFormat numberFormatter = new DecimalFormat("##.####");

    private void writeAggRecord(final Long hashKey, final BufferedWriter writer) {

        
        MetricData metricData = dataCache.get(hashKey);
        try {
            for (String metricName : GlobalRepo.getInstance().getMetrics(aggregateType)) {
                MetricAggregate metricAgg =
                        metricData.getAggData(metricName);
                if (metricAgg != null) {
                    for (AggOperator aggOperator : AggOperator.values()) {

                        switch (aggOperator) {

                            case SUM:

                                writer.write(numberFormatter.format(metricAgg.getSummaryStatistics().getSum()));
                                break;
                            case MIN:

                                writer.write(numberFormatter.format(metricAgg.getSummaryStatistics().getMin()));
                                break;
                            
                            case MAX:

                                writer.write(numberFormatter.format(metricAgg.getSummaryStatistics().getMax()));
                                break;
                            case AVG:
                                writer.write(numberFormatter.format(metricAgg.getSummaryStatistics().getMean()));
                                break;
                        }
                        writer.write(FLD_DELIM);
                    }
                }

            }
            writer.write(""+ metricData.getCount());
           
        }catch (Exception e){
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

    private void dumpMetricData(MetricData metricData) {
        for (String metricName : GlobalRepo.getInstance().getMetrics(aggregateType)) {

            MetricAggregate metricAgg =
                    metricData.getAggData(metricName);
            if (metricAgg != null) {
                System.out.print("Metric: " + metricName + " ");
                SummaryStatistics summaryStatistics = metricAgg.getSummaryStatistics();
                System.out.println(" " + summaryStatistics.getSum() + " " + summaryStatistics.getMin() + " avg is " + summaryStatistics.getMean());
            } else {
                System.out.println("No data for metric " + metricName);
            }
        }

    }
    
    public static void main(String [] args){

        GlobalRepo.getInstance().init();
        CacheStore.getInstance().init();
        final String fileName = "/tmp/fact2.csv";
        List<IAggregator> aggregators = new ArrayList<IAggregator>();
        final String outDir = "/tmp/aggdata/";
        
        IAggregator userAgentAgg = new Aggregator(AggregateType.USER_AGENT,TimeGranularity.MINUTE_OF_DAY,outDir);
        IAggregator geolocationAgg = new Aggregator(AggregateType.GEO_LOCATION,TimeGranularity.MINUTE_OF_DAY,outDir);
        IAggregator geolocationAggHourly = new Aggregator(AggregateType.GEO_LOCATION,TimeGranularity.HOUR_OF_DAY,outDir);
        
        aggregators.add(userAgentAgg);
        aggregators.add(geolocationAgg);
        aggregators.add(geolocationAggHourly);

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            for(IAggregator aggregator: aggregators)
                aggregator.init();
            String header = reader.readLine();
            
            for(IAggregator aggregator: aggregators)
                aggregator.processHeader(header);
            
            String line = reader.readLine();
            while (line != null) {

                String[] recordTokens = line.split(FLD_DELIM);
                
                for(IAggregator aggregator:aggregators)
                    aggregator.processRecord(recordTokens);

                line = reader.readLine();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        for(IAggregator aggregator:aggregators) {

            aggregator.dumpAggregatedData();
            aggregator.postProcess();
        }
        CacheStore.getInstance().close();


    }

}
