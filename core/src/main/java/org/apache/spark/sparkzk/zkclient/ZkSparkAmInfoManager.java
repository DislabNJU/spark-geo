package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.common.IZkClient;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;
import org.apache.spark.sparkzk.zkclient.common.serializer.ObTrans;
import scala.Int;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZkSparkAmInfoManager {
    public final static String infoRootPath = "/sparkNodeInfo";
    private IZkClient zk;
    private String myInfoName = "";
    private String appNodeName = "";
    private int myJobId = 0;
    private int myStageId = 0;
    private String myNodePath = "";
    private String appNodePath = "";
    private NodeData nodeData = null;
    public ZkSparkAmInfoManager(String zkHostName, String appNodeName, String infoName){

        this.nodeData = new NodeData();
        this.appNodeName = appNodeName;
        zk = new ZkClient(zkHostName);

        this.appNodePath = infoRootPath+"/"+appNodeName;
        if(!zk.exists(infoRootPath)) {
            zk.createPersistent(infoRootPath);
        }
        if(!zk.exists(appNodeName)) {
            zk.createPersistent(appNodePath);
        }
        myNodePath = infoRootPath+"/"+appNodeName+"/"+infoName;
        createInfoNode(myNodePath);
        myInfoName = infoName;
    }

    private class NodeData implements Serializable {
        private int jobId ;
        private int stageId ;
        public NodeData(){

        }
        public NodeData(int jobId, int stageId){
            this.jobId = jobId;
            this.stageId = stageId;
        }
        public int getJobId(){
            return this.jobId;
        }
        public int getStageId(){
            return this.stageId;
        }
        public void setJobId(int jobId){
            this.jobId = jobId;
        }
        public void setStageId(int stageId){
            this.stageId = stageId;
        }
    }

    private void createInfoNode(String clientInfoName){
        if(!zk.exists(clientInfoName)) {
            zk.createEphemeral(clientInfoName);
        }
        else{
            System.out.println("create a node: "+clientInfoName+" which has been exit!");
        }
    }

    public void putData(int jobId, int stageId){
        this.myJobId = jobId;
        this.myStageId = stageId;
        nodeData.setJobId(this.myJobId);
        nodeData.setStageId(this.myStageId);
        zk.writeData(myNodePath, ObTrans.ObjectToBytes(nodeData));

    }

    public Map<Integer,Integer[]> getNodeDataInfo(){
        Map<Integer,Integer[]> infoMap = new HashMap<Integer,Integer[]>();
        List<String> allChildNames = getAllNodeName();
        List<Integer> childrenNameInt = new ArrayList<Integer>();
        for(String info : allChildNames){
            int numInfo = Integer.parseInt(info);
            String dataPath = appNodePath+"/"+info;
            NodeData tempNodeData = (NodeData)ObTrans.BytesToObject(zk.readData(dataPath));
            int jobId = tempNodeData.getJobId();
            int stageId = tempNodeData.getStageId();
            Integer[] intArs = new Integer[2];
            intArs[1] = jobId;
            intArs[2] = stageId;
            infoMap.put(numInfo,intArs);
        }
        return infoMap;
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
