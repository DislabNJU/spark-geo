package org.apache.spark.sparkzk.zkclient.common.serializer.containerLaunchContext;

/**
 * Created by ubuntu2 on 9/22/17.
 */
public enum MyApplicationAccessType {

    /**
     * Access-type representing 'viewing' application. ACLs against this type
     * dictate who can 'view' some or all of the application related details.
     */
    VIEW_APP,

    /**
     * Access-type representing 'modifying' application. ACLs against this type
     * dictate who can 'modify' the application for e.g., by killing the
     * application
     */
    MODIFY_APP;
}
