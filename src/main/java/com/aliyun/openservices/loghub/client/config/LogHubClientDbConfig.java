package com.aliyun.openservices.loghub.client.config;

import java.io.Serializable;

public class LogHubClientDbConfig implements Serializable {
	private static final long serialVersionUID = 1971458274348122817L;

	private final String mDbHost;
	private final int mDbPort;
	private final String mDbName;
	private final String mDbUser;
	private final String mDbPassword;
	private final String mWorkerTableName;
	private final String mLeaseTableName;

	public LogHubClientDbConfig(String dbHost, int dbPort, String dbName,
			String dbUser, String dbPassword, String workerTableName,
			String leaseTableName) {
		mDbHost = dbHost;
		mDbPort = dbPort;
		mDbName = dbName;
		mDbUser = dbUser;
		mDbPassword = dbPassword;
		mWorkerTableName = workerTableName;
		mLeaseTableName = leaseTableName;
	}

	public String getDbHost() {
		return mDbHost;
	}

	public int getDbPort() {
		return mDbPort;
	}

	public String getDbName() {
		return mDbName;
	}

	public String getDbUser() {
		return mDbUser;
	}

	public String getDbPassword() {
		return mDbPassword;
	}

	public String getWorkerTableName() {
		return mWorkerTableName;
	}

	public String getLeaseTableName() {
		return mLeaseTableName;
	}
}
