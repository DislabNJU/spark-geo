package org.apache.spark.sparkzk.zkclient.common;

import java.io.Serializable;

public class TestClassToTrans implements Serializable {
    private String name = null;
    public TestClassToTrans(String name){
        this.name = name;
    }
    public void setName(String name){
        this.name = name;
    }
    public String getName(){
        return this.name;
    }
}
