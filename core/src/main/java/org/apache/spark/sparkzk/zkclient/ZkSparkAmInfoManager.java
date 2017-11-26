package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.common.IZkClient;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;

import java.util.List;

public class ZkSparkAmInfoManager {
    public static String infoRootPath = "/sparkNodeInfo";
    private IZkClient zk;
    private String myInfoName = "";
    public ZkSparkAmInfoManager(String zkHostName, String infoName){

        zk = new ZkClient(zkHostName);
        if(!zk.exists(infoRootPath)) {
            zk.createPersistent(infoRootPath);
        }
        createInfoNode(infoName);
        myInfoName = infoName;
    }

    private void createInfoNode(String clientInfoName){
        if(!zk.exists(clientInfoName)) {
            zk.createEphemeral(infoRootPath+"/"+clientInfoName);
        }
        else{
            System.out.println("create a node: "+clientInfoName+" which has been exit!");
        }
    }

    public String getMyInfoName(){
        return this.myInfoName;
    }

    public List<String> getAllNodeName(){
        List<String> names = zk.getChildren(infoRootPath);

        return names;
    }

    public List<String> getOtherNodeName(){
        List<String> names = zk.getChildren(infoRootPath);
        if(names.contains(myInfoName)){
            names.remove(myInfoName);
        }
        return names;
    }
}
