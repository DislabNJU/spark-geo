package org.apache.spark.sparkzk.zkclient.common.serializer.containerLaunchContext;

/**
 * Created by ubuntu2 on 9/24/17.
 */
public enum MyLocalResourceVisibility {
    /**
     * Shared by all users on the node.
     */
    PUBLIC,

    /**
     * Shared among all applications of the <em>same user</em> on the node.
     */
    PRIVATE,

    /**
     * Shared only among containers of the <em>same application</em> on the node.
     */
    APPLICATION
}
