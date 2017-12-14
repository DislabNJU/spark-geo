package org.apache.spark.sparkzk.zkclient.common.serializer.containerLaunchContext;

import java.util.ArrayList;
import java.util.List;

public class ClientArgsList {

    private List<String> userArgs = null;
    public ClientArgsList(List<String> listArgs){
        userArgs = new ArrayList<String>(listArgs);
    }
    public void setUserArgs(List<String> listArgs){
        userArgs =  listArgs;
    }
    public List<String> getUserArgs(){
        return userArgs;
    }
}
