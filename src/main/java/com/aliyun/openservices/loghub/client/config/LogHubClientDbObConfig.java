package com.aliyun.openservices.loghub.client.config;

import java.io.Serializable;

public class LogHubClientDbObConfig implements Serializable {

    private static final long serialVersionUID = -6919543386672198794L;
    private final String      mconfigURL;
    private final String mWorkerTableName;
    private final String mLeaseTableName;
    public LogHubClientDbObConfig(String configURL, String workerTableName,
                                  String leaseTableName){
        mconfigURL = configURL;
        mWorkerTableName = workerTableName;
        mLeaseTableName = leaseTableName;
    }

    public String getConfigURL() {
        return mconfigURL;
    }

    public String getWorkerTableName() {
        return mWorkerTableName;
    }

    public String getLeaseTableName() {
        return mLeaseTableName;
    }

}
