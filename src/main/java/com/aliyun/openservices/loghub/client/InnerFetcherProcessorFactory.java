package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;

public class InnerFetcherProcessorFactory implements ILogHubProcessorFactory {
	
	private final ClientFetcher mFetcher;
	
	public InnerFetcherProcessorFactory(ClientFetcher fetcher) {
		mFetcher = fetcher;
	}
	
	public ILogHubProcessor generatorProcessor()
	{
		return new InnerFetcherProcessor(mFetcher);
	}	

}
