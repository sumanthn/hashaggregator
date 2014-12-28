package sn.analytics.type;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.junit.Assert;
import org.junit.Test;
import sn.analytics.util.KryoPool;

import static org.junit.Assert.*;
/**
 * Created by Sumanth on 27/12/14.
 */
public class GenericGroupByKeyTest  {

    @Test
    public void testBuildKey(){

    }

    public static void main(String [] args){
        String str = "India,Search,Asia";

        System.out.println("string is " + str.getBytes());
        Kryo kryo =KryoPool.getInstance().getKryo();
        ByteBufferOutput bufferOutput = new ByteBufferOutput(20);
        kryo.writeObject(bufferOutput, str);

        byte[] msgBytes = bufferOutput.toBytes();


        System.out.println(msgBytes.length);

        ByteBufferInput input = new ByteBufferInput(msgBytes.length);
        input.setBuffer(msgBytes);
        String str2 =  kryo.readObject(input,String.class);
        System.out.println(str2);
        KryoPool.getInstance().returnToPool(kryo);


    }
}
