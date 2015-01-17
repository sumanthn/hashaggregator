package sn.analytics.aggregator;

import org.slf4j.Logger;
import sn.analytics.GlobalRepo;
import sn.analytics.type.AggregateType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads a file record by record
 * creates a key & reduces
 * Created by Sumanth on 27/12/14.
 */
//TODO: can move to actor model
//parallelize the reading of files
//generate map keys in parallel & reduce in one actor
public class AggregatorDriver {

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(AggregatorDriver.class);
    
    final String inFile;
    final String outDir;

    List<IAggregator> aggregators = new ArrayList<IAggregator>();


    public AggregatorDriver(String inFile, String outDir) {
        this.inFile = inFile;
        this.outDir = outDir;
        GlobalRepo.getInstance().init();
        CacheStore.getInstance().init();
    }
    
    public AggregatorDriver addAggregator(final IAggregator aggregator){
        aggregators.add(aggregator);
        return this;
    }

    public void computeAggregates(){

        logger.info("Compute aggregation from raw fact {} ", inFile);
        long recordCount =0;
        final String FLD_DELIM =",";
        try {
            BufferedReader reader = new BufferedReader(new FileReader(inFile));
            for(IAggregator aggregator: aggregators)
                aggregator.init();
            String header = reader.readLine();

            logger.info("Marking field positions");
            for(IAggregator aggregator: aggregators)
                aggregator.processHeader(header);

            String line = reader.readLine();
            while (line != null) {

                String[] recordTokens = line.split(FLD_DELIM);

                for(IAggregator aggregator:aggregators)
                    aggregator.processRecord(recordTokens);

                line = reader.readLine();
                recordCount++;
                if (recordCount % 10000 == 0)
                    logger.info("Processed {} records" , recordCount);
            }
            
            logger.info("Completed computation of aggregation");

        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    public void dumpAggregatedData(){

        logger.info("Dump aggregated data");
        for(IAggregator aggregator:aggregators) 
            aggregator.dumpAggregatedData();
        
    }
    
    public void cleanup(){
        
        for(IAggregator aggregator:aggregators)
            aggregator.postProcess();
        CacheStore.getInstance().close();


    }

    public static void main(String [] args){
        final String fileName= args[0];
        final String outDir = args[1];
       
        //out dir should really not everywhere 
        IAggregator userAgentAgg = new Aggregator(AggregateType.USER_AGENT,TimeGranularity.MINUTE_OF_DAY,outDir);
        IAggregator geolocationAgg = new Aggregator(AggregateType.GEO_LOCATION,TimeGranularity.MINUTE_OF_DAY,outDir);
        IAggregator geolocationAggHourly = new Aggregator(AggregateType.GEO_LOCATION,TimeGranularity.HOUR_OF_DAY,outDir);

        //for all the data no time granularity
        IAggregator sessionActivityAggregator = new SessionActivityAggregator(outDir);

        AggregatorDriver aggregatorDriver = new AggregatorDriver(fileName,outDir);
        aggregatorDriver.
                addAggregator(userAgentAgg).
                addAggregator(geolocationAgg).
                addAggregator(geolocationAggHourly).addAggregator(sessionActivityAggregator);
        
        aggregatorDriver.computeAggregates();
        aggregatorDriver.dumpAggregatedData();
        aggregatorDriver.cleanup();
        logger.info("Completed aggregation computation, aggregated csv files in {}",outDir);
        

    }


}
