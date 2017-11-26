package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.LeaderElect.LongWorkServer;
import org.apache.spark.sparkzk.zkclient.LeaderElect.RunningData;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;

public class ZkSparkLongElectorClient {
    private String myLocalHostName = null;
    private boolean masterTag = false;
    private ZkClient zkClient;
    private LongWorkServer workServer;
    private RunningData runningData;
    private int session_time_mil = 2000;
    private int connectTimeOutMil = 2000;
    public ZkSparkLongElectorClient(String zkHostName, String myLocalHostName, int masterSelectNum){
        this.myLocalHostName = myLocalHostName;
        zkClient = new ZkClient(zkHostName, session_time_mil, connectTimeOutMil);
        this.runningData = new RunningData(myLocalHostName);
        this.workServer = new LongWorkServer(this.runningData, zkClient, masterSelectNum);
    }


    public void setSelectNum(int num){
        this.workServer.setMasterSelectorNum(num);
    }

    public boolean isMasterExist(){
        return this.workServer.isMasterExist();
    }

    public boolean amIMaster(){
        return this.workServer.iAmMaster();
    }

}
