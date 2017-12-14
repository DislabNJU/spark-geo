package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.common.AmNodeData;
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
    private AmNodeData nodeData = null;
    public ZkSparkAmInfoManager(String zkHostName, String appNodeName, String infoName){

        this.nodeData = new AmNodeData();
        this.appNodeName = appNodeName;
        zk = new ZkClient(zkHostName,3000,3000);

        this.appNodePath = infoRootPath+"/"+appNodeName;
        if(!zk.exists(infoRootPath)) {
            zk.createPersistent(infoRootPath);
        }
        if(!zk.exists(appNodePath)) {
            zk.createPersistent(appNodePath);
        }
        myNodePath = infoRootPath+"/"+appNodeName+"/"+infoName;

        if(!zk.exists(myNodePath)) {
            zk.createEphemeral(myNodePath,ObTrans.ObjectToBytes(nodeData));
        }
        else{
            System.out.println("create a node: "+myNodePath+" which has been exit!");
        }


        myInfoName = infoName;
    }



    private void createInfoNode(String clientInfoName){
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

            AmNodeData tempNodeData = (AmNodeData) ObTrans.BytesToObject(zk.readData(dataPath));

            int jobId = tempNodeData.getJobId();
            int stageId = tempNodeData.getStageId();
            Integer[] intArs = new Integer[2];
            intArs[0] = jobId;
            intArs[1] = stageId;
            //System.out.println(jobId+":"+stageId);
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

