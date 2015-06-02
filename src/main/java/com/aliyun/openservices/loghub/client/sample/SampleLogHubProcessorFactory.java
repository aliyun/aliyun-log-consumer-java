package com.aliyun.openservices.loghub.client.sample;

import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;


public class SampleLogHubProcessorFactory implements ILogHubProcessorFactory {
	public ILogHubProcessor generatorProcessor()
	{
		return new SampleLogHubProcessor();
	}
}
