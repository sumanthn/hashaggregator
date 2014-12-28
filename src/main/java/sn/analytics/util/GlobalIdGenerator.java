package sn.analytics.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Sumanth on 19/11/14.
 */
public class GlobalIdGenerator {
    private AtomicLong counter = new AtomicLong(System.currentTimeMillis());
    private static GlobalIdGenerator ourInstance = new GlobalIdGenerator();

    public static GlobalIdGenerator getInstance() {
        return ourInstance;
    }

    private GlobalIdGenerator() {
    }

    public long getId(){
        return counter.incrementAndGet();
    }


}
