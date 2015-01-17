package sn.analytics.type;

/**
 * Created by Sumanth on 10/12/14.
 */
public enum AggOperator {
    SUM,
    MIN,
    MAX,
    AVG;
    
    public String toString(){
        switch(this){

            case SUM:
                return "sum";
                
            case MIN:
                return "min";
            case MAX:
                return "max";
            case AVG:
                return "avg";
                
        }
        return "unknown";
        
    }
    
    
}
