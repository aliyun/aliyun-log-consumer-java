package com.aliyun.openservices.loghub.client.sample;

import java.util.Scanner;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubClientDbConfig;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;

public class SampleProcessMain {
	public static void main(String args[]) {
		LogHubClientDbConfig dbConfig = new LogHubClientDbConfig(
				"10.101.172.22", 3306, "scmc", "apsara", "123456",
				"loghub_worker", "loghub_lease");

		System.out.println("Please input instancename:");
		Scanner sn = new Scanner(System.in);

		String instanceName = sn.next();

		String project = "loghub-client-worker-test";
		String stream = "test_2_shards";

		LogHubConfig config = new LogHubConfig("consume_2_shards",
				instanceName, "10.101.214.153:60001", project, stream,
				"a7zan0ywbuE794dm", "wxq6YGQ4csLRkCvFeE0HJvZA4oR7A6", dbConfig,
				LogHubCursorPosition.BEGIN_CURSOR);
		config.setDataFetchIntervalMillis(1000);
		ClientWorker worker = new ClientWorker(
				new SampleLogHubProcessorFactory(), config);
		worker.run();

	}
}
