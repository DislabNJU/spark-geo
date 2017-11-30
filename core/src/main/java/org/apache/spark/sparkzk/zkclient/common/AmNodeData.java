package org.apache.spark.sparkzk.zkclient.common;

import java.io.Serializable;

public class AmNodeData implements Serializable {
    private int jobId = 0 ;
    private int stageId = 0 ;
    public AmNodeData(){

    }

    public AmNodeData(int jobId, int stageId){
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
