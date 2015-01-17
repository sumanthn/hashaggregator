package sn.analytics.type;

import java.io.Serializable;

/**
 * Created by Sumanth on 30/12/14.
 */

public class CacheItem implements Serializable {
    
    //this is CRUDE , can be made much better with JSON
    public static final String DELIM = "|";
    //can be list of appenders
    private StringBuilder data = new StringBuilder();
    
    private int count;
    
    public void appendData(final String str){
        data.append(str);
    }
    
    public String getData(){
        
        return data.toString();
    }
    
    public void updateCount(){
        count++;
        
    }

    public int getCount() {
        return count;
    }
}

