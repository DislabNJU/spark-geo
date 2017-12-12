package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.common.IZkClient;
import org.apache.spark.sparkzk.zkclient.common.IZkDataListener;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;


public class ZkSparkRecoveryClient {
    public static String dataRootPath = "/sparkRecovery";
    private IZkClient zk;
    private byte[] dataBuffer = null;
    private IZkDataListener IzkDL;
    private String myDataPath = null;
    private String myNodeName = null;
    public ZkSparkRecoveryClient(String hostName, String appNodeName, byte[] buffer){
        myNodeName = appNodeName;
        String path = dataRootPath+"/"+appNodeName;
        myDataPath = path;

        zk = new ZkClient(hostName);
        if(!zk.exists(dataRootPath)) {
            zk.createPersistent(dataRootPath);
        }

        if(!zk.exists(path)) {
            zk.createPersistent(path);
        }
        else{
            System.out.println("create a node: "+path+" which has been exit!");
        }


        this.dataBuffer = buffer;
        putData(dataBuffer);
    }

    public String  getAppDataNodeName(){
        return myNodeName;
    }

    public String getAppDataDataPath(){
        return myDataPath;
    }

    public void putData(byte[] dataBuffer){
        String path = myDataPath;
        zk.writeData(path, dataBuffer);
    }

    public byte[] getData(){
        return this.dataBuffer;
    }
}
