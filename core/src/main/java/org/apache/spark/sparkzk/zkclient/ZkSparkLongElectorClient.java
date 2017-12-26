package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.LeaderElect.LongWorkServer;
import org.apache.spark.sparkzk.zkclient.LeaderElect.RunningData;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;

public class ZkSparkLongElectorClient {
    private String myClientId = null;
    private boolean masterTag = false;
    private ZkClient zkClient;
    private LongWorkServer workServer;
    private RunningData runningData;
    private int session_time_mil = 2000;
    private int connectTimeOutMil = 3000;
    public ZkSparkLongElectorClient(String zkHostName, String appNodeName, String clientId, Long masterSelectNum){
        this.myClientId = clientId;
        zkClient = new ZkClient(zkHostName, session_time_mil, connectTimeOutMil);
        this.runningData = new RunningData(clientId);
        this.workServer = new LongWorkServer(this.runningData, zkClient, appNodeName, masterSelectNum);
    }


    public void setSelectNum(Long num){
        this.workServer.setMasterSelectorNum(num);
    }

    public boolean isMasterExist(){
        return this.workServer.isMasterExist();
    }

    public boolean amIMaster(){
        return this.workServer.iAmMaster();
    }

    public int getMasterId(){
        return this.workServer.masterId();
    }

}
