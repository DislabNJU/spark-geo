package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.common.*;
import java.util.concurrent.locks.ReentrantLock;

public class ZkSparkDataClient {
    public static String dataRootPath = "/sparkData";
    private IZkClient zk;
    private byte[] dataBuffer = null;
    private IZkDataListener IzkDL;
    private String myDataPath = null;
    private String myNodeName = null;
    private int dataFreshTimeMill = 500;
    private boolean isDataChange = false;
    private ReentrantLock dataLock = new ReentrantLock();
    /*
     *hostname, zookeeper server's host
     * nodename, zonode name
     * dataFreshTimeMill, fresh data time
     */
    public ZkSparkDataClient(String hostName, String appNodeName, int  dataFreshTimeMill){
        zk = new ZkClient(hostName);
        if(!zk.exists(dataRootPath)) {
            zk.createPersistent(dataRootPath);
        }
        this.dataFreshTimeMill = dataFreshTimeMill;

        myNodeName = appNodeName;
        String path = dataRootPath+"/"+appNodeName;
        if(!zk.exists(path)) {
            zk.createPersistent(path);
        }
        else{
            System.out.println("create a node: "+path+" which has been exit!");
        }
        myDataPath = path;

        MonitorDataChange mdc = new MonitorDataChange();
        Thread mdcT = new Thread(mdc);
        mdcT.start();
    }

    private void buildData(byte[] dataBuffer){
        this.dataBuffer = dataBuffer.clone();
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

    private class MonitorDataChange implements Runnable{

        @Override
        public void run() {
            //Random random = new Random();
            IzkDL =  new IZkDataListener() {
                public void handleDataDeleted(String path) throws Exception {
                    System.out.println("data destroy!");
                }
                public void handleDataChange(String path, byte[] data) throws Exception {
                    //System.out.println("data change!!!!!");
                    dataLock.lock();
                    isDataChange = true;
                    buildData(data);
                    dataLock.unlock();
                }
            };
            while(true) {
                zk.subscribeDataChanges(myDataPath, IzkDL);
                try {
                    Thread.sleep(dataFreshTimeMill);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public boolean isDataFresh(){
        boolean is;
        dataLock.lock();
        is = isDataChange;
        dataLock.unlock();
        return is;
    }

    public byte[] getData(){
        dataLock.lock();
        isDataChange = false;
        dataLock.unlock();
        return this.dataBuffer;
    }
}
