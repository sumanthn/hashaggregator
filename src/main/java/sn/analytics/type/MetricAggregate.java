package sn.analytics.type;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.Serializable;

/**
 * Created by Sumanth on 18/11/14.
 */
public class MetricAggregate implements Serializable {


    private SummaryStatistics summaryStatistics=new SummaryStatistics();

    public MetricAggregate(){

    }

    public void insertDataPoint(int val){
        summaryStatistics.addValue(val);
    }


    public void insertDataPoint(long val){
        summaryStatistics.addValue(val);
    }

    public void insertDataPoint(double val){
        summaryStatistics.addValue(val);
    }


    public SummaryData summarize(){

        if (!Double.valueOf(summaryStatistics.getMean()).isNaN() ) {
            SummaryData summaryData = new SummaryData(summaryStatistics.getMin(),
                    summaryStatistics.getMean(), summaryStatistics.getMax(),summaryStatistics.getSum());
            try {
                //valid if there are multiple items

                    double avg = summaryData.getAvg();

                    summaryData.setMedian(avg);
                    summaryData.setPercentile90(avg);
                    summaryData.setPercentile95(avg);


                return summaryData;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return SummaryData.EMPTY_INSTANCE();

    }

    public final SummaryStatistics getSummaryStatistics(){
        return summaryStatistics;
    }
}

