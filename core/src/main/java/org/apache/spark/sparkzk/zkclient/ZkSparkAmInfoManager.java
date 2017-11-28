package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.common.IZkClient;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;
import scala.Int;

import java.util.ArrayList;
import java.util.List;

public class ZkSparkAmInfoManager {
    public final static String infoRootPath = "/sparkNodeInfo";
    private IZkClient zk;
    private String myInfoName = "";
    private String appNodeName = "";
    public ZkSparkAmInfoManager(String zkHostName, String appNodeName, String infoName){
        this.appNodeName = appNodeName;
        zk = new ZkClient(zkHostName);
        if(!zk.exists(infoRootPath)) {
            zk.createPersistent(infoRootPath);
        }
        if(!zk.exists(appNodeName)) {
            zk.createPersistent(infoRootPath+"/"+appNodeName);
        }
        createInfoNode(infoRootPath+"/"+appNodeName+"/"+infoName);
        myInfoName = infoName;
    }

    private void createInfoNode(String clientInfoName){
        if(!zk.exists(clientInfoName)) {
            zk.createEphemeral(clientInfoName);
        }
        else{
            System.out.println("create a node: "+clientInfoName+" which has been exit!");
        }
    }

    public String getMyInfoName(){
        return this.myInfoName;
    }

    public List<String> getAllNodeName(){

        String path = infoRootPath+"/"+appNodeName;
        List<String> names = zk.getChildren(path);
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
