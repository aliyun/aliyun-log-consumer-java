package com.aliyun.openservices.loghub.client.sample;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.PropertyConfigurator;

import com.aliyun.openservices.sls.SLSClient;
import com.aliyun.openservices.sls.common.LogItem;
import com.aliyun.openservices.sls.exception.SlsException;

public class SampleSendData {
	static {
		PropertyConfigurator.configure("config" + File.separator + "log4j.properties");
	}

	private static Scanner sn;

	public static void main(String args[]) throws IOException {
		SLSClient loghubClient = new SLSClient("cn-hangzhou-devcommon-intranet.sls.aliyuncs.com",
				"94to3z418yupi6ikawqqd370", "DFk3ONbf81veUFpMg7FtY0BLB2w=");
		String project = "ali-yun-xgl";
		String stream = "logstore-xgl";		
		
		int index = 0;
		sn = new Scanner(System.in);
		int total_suc = 0;
		while (true) {
			System.out.println("Input loggroup count to send:");
			int num = sn.nextInt();

			for (int i = 0; i < num; i++ , index++) {			
				List<LogItem> logItems = new ArrayList<LogItem>();
				LogItem item = new LogItem();	
				
				String key = "key_" + String.valueOf(index);
				String val = "value_" + String.valueOf(index);
				
				item.PushBack(key, "value_" + val);
				logItems.add(item);
				
				try {
					loghubClient.PutLogs(project, stream, "", logItems, "");
					total_suc += 1;
				} catch (SlsException e) {
					e.printStackTrace();
				} 
				
				System.out.println("key: " + key + " val: " + val);
			}
			System.out.println("Total success:" + String.valueOf(total_suc));
		}
	}
}
