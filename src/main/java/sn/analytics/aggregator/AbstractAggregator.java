package sn.analytics.aggregator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.infinispan.Cache;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sn.analytics.GlobalRepo;
import sn.analytics.type.AggregateType;
import sn.analytics.type.GenericGroupByKey;
import sn.analytics.type.MetricData;

import java.util.*;

/**
 * Aggregate based on hash key
 * Created by Sumanth on 27/12/14.
 */
public class AbstractAggregator {
    private static final Logger logger = LoggerFactory.getLogger(AbstractAggregator.class);
    static final DateTimeFormatter MILL_SECONDS_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    static final DateTimeFormatter SECONDS_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    static final DateTimeFormatter MINUTES_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");

    static final DateTimeFormatter MINUTES_TRUNCATED_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm");
    static final DateTimeFormatter HOUR_TRUNCATED_FORMAT = DateTimeFormat.forPattern("yyyyMMddHH");
    static final DateTimeFormatter DAY_TRUNCATED_FORMAT = DateTimeFormat.forPattern("yyyyMMdd");


    static final String FLD_DELIM=",";

    protected int INVALID_MARKER=-1;
    //mark the positions of metrics and dimensions in the CSV file
    protected Map<String,Integer> metricPositions;
    protected ImmutableList<Integer> dimPositions;
    protected Cache <Long,GenericGroupByKey> keyCache;


    protected Map<Long,String> dateTimeCache = new HashMap<Long, String>();
    protected Map<String,Integer> timeDimensionsPositions = new HashMap<String, Integer>();
    protected HashFunction hashFunction   = Hashing.murmur3_128(new Random().nextInt());

    protected long processedRecord = 0;
    
    protected void discoverPositions(final String header,final List<String> dimensions, final List<String> metrics){
        String [] tkns = header.split(FLD_DELIM);


        Map<String,Integer> positions = new HashMap<String, Integer>();
        for(int i=0;i< tkns.length;i++){
            positions.put(tkns[i],i);

        }

       ImmutableList.Builder<Integer> builderObj =  ImmutableList.builder();
        for(String dim : dimensions) {
            if (positions.containsKey(dim))
                builderObj.add(positions.get(dim));
            else
                builderObj.add(INVALID_MARKER);

        }

        dimPositions = builderObj.build();

        builderObj =  ImmutableList.builder();
        for(String metric: metrics){
            if (positions.containsKey(metric))
                builderObj.add(positions.get(metric));
            else
               builderObj.add(INVALID_MARKER);

        }

        ImmutableMap.Builder<String,Integer> mapMaker  = ImmutableMap.builder();

        for(String metric: metrics){
            if (positions.containsKey(metric))
                mapMaker.put(metric,positions.get(metric));
            else
               mapMaker.put(metric,INVALID_MARKER);
        }

        metricPositions = mapMaker.build();

        Map<String,String> timeDimData =  GlobalRepo.getInstance().getTimeDimensions();
        for(String name : timeDimData.keySet()){
            String fldName = timeDimData.get(name);
            if (positions.containsKey(fldName)){
                timeDimensionsPositions.put(name,positions.get(fldName));
            }else {
                timeDimensionsPositions.put(name,INVALID_MARKER);
            }
        }



    }

    public Map<String,Integer> getMetricPositions() {
        return metricPositions;
    }

    public ImmutableList<Integer> getDimPositions() {
        return dimPositions;
    }

    public Map<String, Integer> getTimeDimensionsPositions() {
        return timeDimensionsPositions;
    }

    public static void main(String [] args){
        GlobalRepo.getInstance().init();
        String header = "accessUrl,responseStatusCode,responseTime,receivedTimestamp,requestVerb,requestSize,dataExchangeSize,serverIp,clientIp,clientId,sessionId,userAgentDevice,UserAgentType,userAgentFamily,userAgentOSFamily,userAgentVersion,userAgentOSVersion,city,country,region,minOfDay,hourOfDay,dayOfWeek,monthOfYear";

        AbstractAggregator abstractAggregator = new AbstractAggregator();
        abstractAggregator.discoverPositions(header,
                GlobalRepo.getInstance().getDimensions(AggregateType.CLIENT_SESSION),
                GlobalRepo.getInstance().getMetrics(AggregateType.CLIENT_SESSION));
        
        Set<String> metricNames = abstractAggregator.getMetricPositions().keySet();
        for(String metricName : metricNames){
            System.out.println(metricName + " " +abstractAggregator.getMetricPositions().get(metricName));
        }
    }
    protected static DateTime strToDateTime(final String str) {
        if (str.length() == 19) {
            return DateTime.parse(str, SECONDS_FORMAT);

        } else if (str.length() == 23) {
            return DateTime.parse(str, MILL_SECONDS_FORMAT);
        }
        return null;
    }




}
