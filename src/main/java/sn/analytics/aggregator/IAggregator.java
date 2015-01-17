package sn.analytics.aggregator;

/**
 * Created by Sumanth on 28/12/14.
 */
public interface IAggregator {

    public void init();
    //marks the positions
    public void processHeader(final String header);
    //takes record tokens and processes the aggregates
    public void processRecord(final String [] recordTokens);
    /* dumps all the aggregated data*/
    public String dumpAggregatedData();
    //close resources and cleanup
    public void postProcess();
}
