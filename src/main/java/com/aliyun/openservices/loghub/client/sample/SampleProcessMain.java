package com.aliyun.openservices.loghub.client.sample;

import java.io.File;
import java.util.Scanner;

import org.apache.log4j.PropertyConfigurator;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubClientDbConfig;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;

public class SampleProcessMain {
	
	static {
		PropertyConfigurator.configure("config" + File.separator + "log4j.properties");
	}
	   
	public static void main(String args[]) {
		LogHubClientDbConfig dbConfig = new LogHubClientDbConfig(
				"10.101.172.22", 3306, "scmc", "apsara", "123456",
				"loghub_worker", "loghub_lease");

		System.out.println("Please input instancename:");
		Scanner sn = new Scanner(System.in);

		String instanceName = sn.next();
		
		String project = "ali-yun-xuguilin-test";
		String stream = "xgl-test";

		LogHubConfig config = new LogHubConfig("consume_10_shards",
				instanceName, "cn-hangzhou-staging-intranet.sls.aliyuncs.com", project, stream,
				"rDwmctL3ImDUh01b", "eDEQO0CUbw6j3y0bDgLLOhxrSXxCZ0", dbConfig,
				LogHubCursorPosition.BEGIN_CURSOR);
		config.setDataFetchIntervalMillis(1000);
		ClientWorker worker = new ClientWorker(
				new SampleLogHubProcessorFactory(), config);
		worker.run();

		sn.close();
	}
}
