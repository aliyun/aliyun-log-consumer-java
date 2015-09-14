package com.aliyun.openservices.loghub.client.sample;

import java.io.File;
import java.util.Scanner;

import org.apache.log4j.PropertyConfigurator;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubClientDbConfig;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.lease.impl.MySqlLogHubLeaseManager;

public class SampleProcessMain {

	static {
		PropertyConfigurator.configure("config" + File.separator
				+ "log4j.properties");
	}

	public static void main(String args[]) {
		LogHubClientDbConfig dbConfig = new LogHubClientDbConfig(
				"10.101.172.22", 3306, "db_name", "db_user", "db_password",
				"loghub_worker", "loghub_lease");

		System.out.println("Please input instancename:");
		Scanner sn = new Scanner(System.in);

		String consumer_group_name = "stt-test-10";
		String instanceName = sn.next();

		String project = "your_project_name";

		String logstore = "your_logstore";

		LogHubConfig config = new LogHubConfig(consumer_group_name,
				instanceName, "cn-hangzhou-staging-intranet.sls.aliyuncs.com",
				project, logstore, "your_access_id", "your_access_key",
				LogHubCursorPosition.END_CURSOR);
		config.setDataFetchIntervalMillis(1000);
		MySqlLogHubLeaseManager leaseManager = new MySqlLogHubLeaseManager(
				dbConfig);
		ClientWorker worker = new ClientWorker(
				new SampleLogHubProcessorFactory(), config, leaseManager);
		worker.run();

		sn.close();
	}
}
