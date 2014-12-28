package sn.analytics;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import sn.analytics.aggregator.AggregatorMeta;
import sn.analytics.type.AggregateType;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Sumanth on 27/12/14.
 */
public class GlobalRepo {
    private static GlobalRepo ourInstance = new GlobalRepo();

    public static GlobalRepo getInstance() {
        return ourInstance;
    }

    private ImmutableMap<AggregateType,AggregatorMeta> metaData ;
    private ImmutableMap<String,String> timeDimensionFields;
    private GlobalRepo() {
    }

    public void init(){
        Gson gson = new Gson();
        //read the meta data files and fill the dimensions & metrics
        InputStream userAgentMetaFile = AggregatorMeta.class.getResourceAsStream("/UserAgentAggregateMeta.json");
        AggregatorMeta userAgentAgg = gson.fromJson(new InputStreamReader(userAgentMetaFile),AggregatorMeta.class);

        InputStream geoLocationMetaFile = AggregatorMeta.class.getResourceAsStream("/GeoLocationAggregateMeta.json");
        AggregatorMeta geoLocationAgg = gson.fromJson(new InputStreamReader(geoLocationMetaFile),AggregatorMeta.class);

        InputStream clientMeta = AggregatorMeta.class.getResourceAsStream("/ClientAggregationMeta.json");
        AggregatorMeta clientAgg = gson.fromJson(new InputStreamReader(clientMeta),AggregatorMeta.class);

        metaData = ImmutableMap.of(
                AggregateType.USER_AGENT,userAgentAgg,
                AggregateType.GEO_LOCATION,geoLocationAgg,
                AggregateType.CLIENT_SESSION,clientAgg
                );
        InputStream timeDimensionData = AggregatorMeta.class.getResourceAsStream("/TimeDimensionData.json");
        //AggregatorMeta userAgentAgg = gson.fromJson(new InputStreamReader(timeDimensionData));

        Type type = new TypeToken<Map<String, String>>(){}.getType();
        Map<String, String> timeDimMap = gson.fromJson(new InputStreamReader(timeDimensionData), type);
        timeDimensionFields = ImmutableMap.copyOf(timeDimMap);


    }

    public List<String> getDimensions(final AggregateType aggregateType){
        if (metaData == null) System.out.println("MET AIS NULL");
        return metaData.get(aggregateType).getDimensions();
    }

    public List<String> getMetrics(final AggregateType aggregateType){
        return metaData.get(aggregateType).getMetrics();
    }

    public Map<String,String> getTimeDimensions(){
        return timeDimensionFields;
    }

    public static void main(String [] args){
        GlobalRepo globalRepo = new GlobalRepo();
        globalRepo.init();
        Map<String,String> timeDimensions = new HashMap<String, String>();
        timeDimensions.put("EVENT_TIMESTAMP","receivedTimestamp");
        timeDimensions.put("MIN_OF_DAY_FLD","minOfDay");

       Gson gson = new Gson();

       String jsonStr = gson.toJson(timeDimensions);
        System.out.println(jsonStr);

        Map<String,String> timeDim = globalRepo.getTimeDimensions();
        for(String name: timeDim.keySet()){
            System.out.println("Field:" + name + " " + timeDim.get(name));
        }

    }
}
