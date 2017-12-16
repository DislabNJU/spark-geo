package org.apache.spark.sparkzk.zkclient.common;

import java.io.Serializable;

public class AmRecoveryHostData implements Serializable{
    private String recoveryAmRmHost = "";
    public AmRecoveryHostData(String rmHost){
        this.recoveryAmRmHost = rmHost;
    }
    public void setRecoveryAmRmHost(String rmHost){
        this.recoveryAmRmHost = rmHost;
    }
    public String getRecoveryAmRmHost(){
        return this.recoveryAmRmHost;
    }
}
