package org.apache.spark.sparkzk.zkclient.common.serializer;



import java.io.*;

/**
 * Created by ubuntu2 on 6/19/17.
 */
public class ObTrans {
    //byte to object
    public static Object BytesToObject(byte[] bytes) {
        Object obj = null;
        if(bytes == null){
            return null;
        }
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }
    //object to byte
    public static byte[] ObjectToBytes(Object obj) {
        byte[] bytes = null;
        if(obj ==null){
            return null;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

}
