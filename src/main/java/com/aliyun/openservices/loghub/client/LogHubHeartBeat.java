package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class LogHubHeartBeat {
	private ScheduledExecutorService threadpool;
	private LogHubClientAdapter mLogHubClientAdapter;
	private boolean running = false;
	private final long mHeartBeatIntervalMillis;
	private ArrayList<Integer> mHeldShards;
	private HashSet<Integer> mHeartShards;
	private static final Logger logger = Logger.getLogger(LogHubHeartBeat.class);
	private static final long STOP_WAIT_TIME_MILLIS = 2000L;
	public LogHubHeartBeat(LogHubClientAdapter logHubClientAdapter,
			long heartBeatIntervalMillis) {
		super();
		this.mLogHubClientAdapter = logHubClientAdapter;
		this.mHeartBeatIntervalMillis = heartBeatIntervalMillis;
		mHeldShards = new ArrayList<Integer>();
		mHeartShards = new HashSet<Integer>();
	}
	public void Start()
	{
		threadpool = Executors.newScheduledThreadPool(1);
		threadpool.scheduleWithFixedDelay(new HeartBeatRunnable(), 0L,
				mHeartBeatIntervalMillis, TimeUnit.MILLISECONDS);
		running = true;
	}
	/**
	 * Stops background threads.
	 */
	public void Stop() {
		if (threadpool != null) {
			threadpool.shutdown();
			try {
				if (threadpool.awaitTermination(STOP_WAIT_TIME_MILLIS,
						TimeUnit.MILLISECONDS)) {
				} else {
					threadpool.shutdownNow();

				}
			} catch (InterruptedException e) {

			}
		}
		running = false;
	}
	synchronized public void GetHeldShards(ArrayList<Integer> shards)
	{
		shards.clear();
		shards.addAll(mHeldShards);
	}
	synchronized public void RemoveHeartShard(final int shard)
	{
		mHeartShards.remove((Integer)shard);
	}
	synchronized protected void HeartBeat()
	{
		if(mLogHubClientAdapter.HeartBeat(new ArrayList<Integer>(mHeartShards), mHeldShards))
		{
			mHeartShards.addAll(mHeldShards);
		}
	}
	private class HeartBeatRunnable implements Runnable
	{
		@Override
		public void run() {
			try {
				HeartBeat();
			}
			catch (Throwable t) {
			}
		}	
	}
	
	/**
	 * @return true if this LeaseCoordinator is running
	 */
	public boolean isRunning() {
		return running;
	}
}
