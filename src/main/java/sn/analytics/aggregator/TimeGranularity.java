package sn.analytics.aggregator;

/**
 * Created by Sumanth on 28/12/14.
 */
public enum TimeGranularity {
    MINUTE_OF_DAY,
    HOUR_OF_DAY,
    DAY,
    ALL;
    
    public String toString(){
        switch(this){

            case MINUTE_OF_DAY:
                return "MinuteOfDay";
            
            case HOUR_OF_DAY:
                return "HourOfDay";
            
            case DAY:
                return "Day";
            
            
        }
        
        return "Timestamp";
        
    }
}
