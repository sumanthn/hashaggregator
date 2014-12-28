package sn.analytics.aggregator;

/**
 * Created by Sumanth on 28/12/14.
 */
public interface IAggregator {

    public void init();
    public void processHeader(final String header);
    //a convince split into tokens
    public void processRecord(final String [] recordTokens);
    public void postProcess();
}
