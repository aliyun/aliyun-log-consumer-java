package com.aliyun.openservices.loghub.client.sample;

import java.io.IOException;
import java.util.Scanner;

import com.aliyun.openservices.loghub.LogHubClient;
import com.aliyun.openservices.loghub.common.LogGroup;
import com.aliyun.openservices.loghub.common.LogItem;
import com.aliyun.openservices.loghub.common.LogMeta;
import com.aliyun.openservices.loghub.exception.LogHubClientException;
import com.aliyun.openservices.loghub.exception.LogHubException;

public class SampleSendData {

	private static Scanner sn;

	public static void main(String args[]) throws IOException {
		LogHubClient loghubClient = new LogHubClient("10.101.214.153:60001",
				"a7zan0ywbuE794dm", "wxq6YGQ4csLRkCvFeE0HJvZA4oR7A6");
		String project = "loghub-client-worker-test";
		String stream = "test_2_shards";

		
		int index = 0;
		sn = new Scanner(System.in);
		int total_suc = 0;
		while (true) {
			System.out.println("Input loggroup count to send:");
			int num = sn.nextInt();
			for (int i = 0; i < num; i++ , index++) {
				LogGroup logGroup = new LogGroup(new LogMeta());
				LogItem item = new LogItem(System.currentTimeMillis());
				item.append("key_" + String.valueOf(index), "value_" + String.valueOf(index));
				
				logGroup.addLog(item);
				try {
					loghubClient.sendData(project, stream, logGroup);
					total_suc += 1;
				} catch (LogHubException e) {
					e.printStackTrace();
				} catch (LogHubClientException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Total success:" + String.valueOf(total_suc));
		}
	}
}
