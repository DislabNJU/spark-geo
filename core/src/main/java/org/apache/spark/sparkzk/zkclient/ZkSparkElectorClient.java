package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.LeaderElect.*;
import org.apache.spark.sparkzk.zkclient.common.*;


public class ZkSparkElectorClient {

    private String hostName = null;
    private boolean masterTag = false;
    private ZkClient zkClient;
    private WorkServer workServer;
    private RunningData runningData;
    public ZkSparkElectorClient(String zkHostName, String myLocalHostName, int masterFreshTimeMil){
        this.hostName = zkHostName;
        zkClient = new ZkClient(this.hostName);
        this.runningData = new RunningData(myLocalHostName);
        this.workServer = new WorkServer(this.runningData, zkClient,masterFreshTimeMil);
    }


    public String getMasterName(){
        return this.workServer.masterName();
    }

    /*protected void finalize( ) {
        System.out.println("ZkSparkElectorClient shut down!");
        workServer.stop();
    }*/
}
