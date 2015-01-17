package sn.analytics.aggregator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Meta data store for aggregates
 * Created by Sumanth on 27/12/14.
 */
public class AggregatorMeta {

    private List<String> dimensions = new ArrayList<String>();
    private List<String> metrics = new ArrayList<String>();

    public List<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }

    public List<String> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<String> metrics) {
        this.metrics = metrics;
    }

    public static void main(String [] args){
        AggregatorMeta aggregatorJson = new AggregatorMeta();

        Gson gson = new Gson();
        InputStream userAgentFormatFile = AggregatorMeta.class.getResourceAsStream("/UserAgentAggregateMeta.json");
        AggregatorMeta meta2 = gson.fromJson(new InputStreamReader(userAgentFormatFile),AggregatorMeta.class);
        for(String dim : meta2.dimensions)
            System.out.println(dim);

        for(String metric : meta2.metrics)
            System.out.println(metric);


    }
}
