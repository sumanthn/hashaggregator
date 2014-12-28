package sn.analytics.type;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import sn.analytics.util.KryoPool;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by Sumanth on 27/12/14.
 */
public class GenericGroupByKey implements Comparable<GenericGroupByKey>, Serializable{

    private final long hashKey;
    private final byte [] groupByKey;

    public GenericGroupByKey(long hashKey, byte[] groupByKey) {
        this.hashKey = hashKey;
        this.groupByKey = groupByKey;
    }

    public long getHashKey() {
        return hashKey;
    }

    public byte[] getGroupByKey() {
        return groupByKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericGroupByKey)) return false;

        GenericGroupByKey that = (GenericGroupByKey) o;

        if (hashKey != that.hashKey) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (hashKey ^ (hashKey >>> 32));
    }

    @Override
    public int compareTo(GenericGroupByKey o) {
        return Long.valueOf(this.hashKey).compareTo(o.getHashKey());
    }

    public static String getStringFromBytes(final byte [] bytes){

        Kryo kryo = KryoPool.getInstance().getKryo();
        ByteBufferInput in = new ByteBufferInput(bytes.length+5);
        in.setBuffer(bytes);
        String str = kryo.readObject(in,String.class);
        in.close();
        KryoPool.getInstance().returnToPool(kryo);
        return str;
    }
}
