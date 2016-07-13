package com.aliyun.openservices.loghub.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;

public class LogHubConsumer {
	enum ConsumerStatus {
		INITIALIZING, PROCESSING, SHUTTING_DOWN, SHUTDOWN_COMPLETE
	}
	private int mShardId;
	private String mInstanceName;
	private LogHubClientAdapter mLogHubClientAdapter;
	private DefaultLogHubCheckPointTracker mCheckPointTracker;
	private ILogHubProcessor mProcessor;
	private LogHubCursorPosition mCursorPosition;
	private int mCursorStartTime = 0;
	
	private ConsumerStatus mCurStatus = ConsumerStatus.INITIALIZING;

	private ITask mCurrentTask;
	private Future<TaskResult> mTaskFuture;
	private Future<TaskResult> mFetchDataFeture;

	private ExecutorService mExecutorService;
	private String mNextFetchCursor;
	private boolean mShutDown = false;

	private FetchedLogGroup mLastFetchedData;
	
	private static final Logger logger = Logger.getLogger(LogHubConsumer.class);
	private long mLastLogErrorTime = 0;
	private long mLastFetchTime = 0;
	private int mLastFetchCount = 0;
	private int mLastFetchRawSize = 0;
	public LogHubConsumer(LogHubClientAdapter logHubClientAdapter,int shardId, String instanceName,
			ILogHubProcessor processor,
			ExecutorService executorService,  LogHubCursorPosition cursorPosition, int cursorStartTime) {
		mLogHubClientAdapter = logHubClientAdapter;
		mShardId = shardId;
		mInstanceName = instanceName;
		mCursorPosition = cursorPosition;
		mCursorStartTime = cursorStartTime;
		mProcessor = processor;
		mCheckPointTracker = new DefaultLogHubCheckPointTracker(logHubClientAdapter,
				mInstanceName, mShardId);
		mExecutorService = executorService;

	}

	public void consume() {
		checkAndGenerateNextTask();
		if (this.mCurStatus.equals(ConsumerStatus.PROCESSING)
				&& mLastFetchedData == null) {
			fetchData();
		}
	}
	
	public void saveCheckPoint(String cursor, boolean persistent) 
			throws LogHubCheckPointException {		
		mCheckPointTracker.saveCheckPoint(cursor, persistent);		
	}

	private void checkAndGenerateNextTask() {
		if (mTaskFuture == null || mTaskFuture.isCancelled()
				|| mTaskFuture.isDone()) {
			boolean taskSuccess = false;
			TaskResult result = getTaskResult(mTaskFuture);
			mTaskFuture = null;
			if (result != null && result.getException() == null) {
				taskSuccess = true;
				if (mCurStatus.equals(ConsumerStatus.INITIALIZING)) {
					InitTaskResult initResult = (InitTaskResult) (result);
					mNextFetchCursor = initResult.getCursor();
					mCheckPointTracker.setInMemoryCheckPoint(mNextFetchCursor);
					if(initResult.isCursorPersistent())
					{
						mCheckPointTracker.setInPeristentCheckPoint(mNextFetchCursor);
					}
				}
				else if (result instanceof ProcessTaskResult)
				{
					ProcessTaskResult process_task_result = (ProcessTaskResult)(result);
					String roll_back_checkpoint = process_task_result.getRollBackCheckpoint();
					if (roll_back_checkpoint != null && roll_back_checkpoint.isEmpty() == false)
					{
						mLastFetchedData = null;
						CancelCurrentFetch();
						mNextFetchCursor = roll_back_checkpoint;
					}
				}
			}
			sampleLogError(result);
			updateStatus(taskSuccess);
			generateNextTask();
		}
	}

	private void fetchData() {
		if (mFetchDataFeture == null || mFetchDataFeture.isCancelled()
				|| mFetchDataFeture.isDone()) {
			TaskResult result = getTaskResult(mFetchDataFeture);
			if (result != null && result.getException() == null) 
			{
				FetchTaskResult fetchResult = (FetchTaskResult) result;
				mLastFetchedData = new FetchedLogGroup(mShardId,
						fetchResult.getFetchedData(), fetchResult.getCursor());
				mNextFetchCursor = fetchResult.getCursor();
				mLastFetchCount = mLastFetchedData.mFetchedData.size();
				mLastFetchRawSize = fetchResult.getRawSize();
			}
			
			sampleLogError(result);
			
			if (result == null || result.getException() == null) 
			{
				boolean genFetchTask = true;
				if(mLastFetchRawSize < 1024 * 1024 && mLastFetchCount < 100)
				{
					genFetchTask = (System.currentTimeMillis() - mLastFetchTime > 500);
				}
				else if(mLastFetchRawSize < 2 * 1024 * 1024 && mLastFetchCount < 500)
				{
					genFetchTask = (System.currentTimeMillis() - mLastFetchTime > 200);
				}
				else if(mLastFetchRawSize < 4 * 1024 * 1024 && mLastFetchCount < 1000)
				{
					genFetchTask = (System.currentTimeMillis() - mLastFetchTime > 50);
				}
				if(genFetchTask)
				{
					mLastFetchTime = System.currentTimeMillis();
					LogHubFetchTask task = new LogHubFetchTask(mLogHubClientAdapter,mShardId, mNextFetchCursor);
					mFetchDataFeture = mExecutorService.submit(task);
				}
				else
				{
					mFetchDataFeture = null;
				}
			}
			else
			{
				mFetchDataFeture = null;
			}
			
		}
	}

	private void sampleLogError(TaskResult result) {
		if (result != null && result.getException() != null) {
			long curTime = System.currentTimeMillis();
			if (curTime - mLastLogErrorTime > 5 * 1000) {
				logger.warn(result.getException());
				mLastLogErrorTime = curTime;
			}
		}
	}

	private TaskResult getTaskResult(Future<TaskResult> future) {
		if (future != null && (future.isDone() || future.isCancelled())) {
			try {
				return future.get();
			} catch (Exception e) {
			}
		}
		return null;

	}
	private void CancelCurrentFetch()
	{
		if (mFetchDataFeture != null) {
			mFetchDataFeture.cancel(true);
			getTaskResult(mFetchDataFeture);
			logger.warn("Cancel a fetch task, shard id:" + mShardId);
			mFetchDataFeture = null;
		}
	}
	
	private void generateNextTask() {
		ITask nextTask = null;
		if (this.mCurStatus.equals(ConsumerStatus.INITIALIZING)) {
			nextTask = new InitializeTask(mProcessor,mLogHubClientAdapter, mShardId, mCursorPosition , mCursorStartTime);
		} else if (this.mCurStatus.equals(ConsumerStatus.PROCESSING)) {
			if (mLastFetchedData != null) {
				mCheckPointTracker.setCursor(mLastFetchedData.mEndCursor);
				nextTask = new ProcessTask(mProcessor,
						mLastFetchedData.mFetchedData, mCheckPointTracker);
				mLastFetchedData = null;
			}
		} else if (this.mCurStatus.equals(ConsumerStatus.SHUTTING_DOWN)) {
			nextTask = new ShutDownTask(mProcessor, mCheckPointTracker);
			CancelCurrentFetch();
		}
		if (nextTask != null) {
			mCurrentTask = nextTask;
			mTaskFuture = mExecutorService.submit(mCurrentTask);
		}
	}

	private void updateStatus(boolean taskSuccess) {
		if (mCurStatus.equals(ConsumerStatus.SHUTTING_DOWN)) {
			if (mCurrentTask == null || taskSuccess) {
				mCurStatus = ConsumerStatus.SHUTDOWN_COMPLETE;
			}
		} else if (mShutDown) {
			mCurStatus = ConsumerStatus.SHUTTING_DOWN;
		} else if (taskSuccess) {
			if (mCurStatus.equals(ConsumerStatus.INITIALIZING)) {
				mCurStatus = ConsumerStatus.PROCESSING;
			}
		}
	}

	public void shutdown() {
		this.mShutDown = true;
		if (!isShutdown()) {
			checkAndGenerateNextTask();
		}
	}

	public boolean isShutdown() {
		return mCurStatus.equals(ConsumerStatus.SHUTDOWN_COMPLETE);
	}

}
