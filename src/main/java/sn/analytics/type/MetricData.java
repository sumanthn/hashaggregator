package sn.analytics.type;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Sumanth on 07/11/14.
 */
public class MetricData implements Serializable {
    private Map<String,MetricAggregate> metricDataMap = new HashMap<String, MetricAggregate>();
    private int count;

    public MetricData(final List<String> metricNames){
        for(String metricName : metricNames)
        metricDataMap.put(metricName,new MetricAggregate());
    }

    public Map<String, MetricAggregate> getMetricDataMap() {
        return metricDataMap;
    }

    public void setMetricDataMap(Map<String, MetricAggregate> metricDataMap) {
        this.metricDataMap = metricDataMap;
    }
    public MetricAggregate getAggData(final String metricName){
        return metricDataMap.get(metricName);
    }

    public void updateDataPoint(final String metric, final int dataPoint){
        metricDataMap.get(metric).insertDataPoint(dataPoint);
    }
    public void updateDataPoint(final String metric, final long dataPoint){
        metricDataMap.get(metric).insertDataPoint(dataPoint);
    }

    public void updateDataPoint(final String metric, final double dataPoint){

        metricDataMap.get(metric).insertDataPoint(dataPoint);
    }

    public void updateCount(){
        count++;
    }

    public int getCount(){return count;}


}

