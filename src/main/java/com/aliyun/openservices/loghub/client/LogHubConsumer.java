package com.aliyun.openservices.loghub.client;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.LogHubClient;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.lease.impl.MySqlLogHubLeaseManager;
import com.aliyun.openservices.loghub.common.LogGroup;
import com.aliyun.openservices.loghub.common.LogHubModes.LogHubMode;

public class LogHubConsumer {
	enum ConsumerStatus {
		INITIALIZING, PROCESSING, SHUTTING_DOWN, SHUTDOWN_COMPLETE
	}

	class FetchedLogGroup {
		public String mEndCursor;
		public List<LogGroup> mFetchedData;

		public FetchedLogGroup(List<LogGroup> fetchedData, String endCursor) {
			mFetchedData = fetchedData;
			mEndCursor = endCursor;
		}
	}

	private String mShardId;
	private String mInstanceName;
	private LogHubClient mLogHubClient;
	private String mProject;
	private String mLogStream;
	private DefaultLogHubCHeckPointTracker mCheckPointTracker;
	private MySqlLogHubLeaseManager mLeaseManager;
	private ILogHubProcessor mProcessor;
	LogHubCursorPosition mCursorPosition;
	
	private ConsumerStatus mCurStatus = ConsumerStatus.INITIALIZING;

	private ITask mCurrentTask;
	private Future<TaskResult> mTaskFuture;
	private Future<TaskResult> mFetchDataFeture;

	private ExecutorService mExecutorService;
	private String mNextFetchCourse;
	private LogHubMode mNextFetchMode;
	private boolean mShutDown = false;

	private FetchedLogGroup mLastFetchedData;
	
	protected Logger logger = Logger.getLogger(this.getClass());
	private long mLastLogErrorTime = 0;

	public LogHubConsumer(LogHubClient loghubClient, String project,
			String logStream, String shardId, String instanceName,
			MySqlLogHubLeaseManager leaseManager, ILogHubProcessor processor,
			ExecutorService executorService,  LogHubCursorPosition cursorPosition) {
		mLogHubClient = loghubClient;
		mProject = project;
		mLogStream = logStream;
		mShardId = shardId;
		mInstanceName = instanceName;
		mCursorPosition = cursorPosition;
		mLeaseManager = leaseManager;
		mProcessor = processor;
		mCheckPointTracker = new DefaultLogHubCHeckPointTracker(mLeaseManager,
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

	private void checkAndGenerateNextTask() {
		if (mTaskFuture == null || mTaskFuture.isCancelled()
				|| mTaskFuture.isDone()) {
			boolean taskSuccess = false;
			TaskResult result = getTaskResult(mTaskFuture);
			if (result != null && result.getException() == null) {
				taskSuccess = true;
				if (mCurStatus.equals(ConsumerStatus.INITIALIZING)) {
					InitTaskResult initResult = (InitTaskResult) (result);
					mNextFetchCourse = initResult.getCursor();
					mNextFetchMode = initResult.getMode();
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
			if (result != null && result.getException() == null) {
				FetchTaskResult fetchResult = (FetchTaskResult) result;
				mLastFetchedData = new FetchedLogGroup(
						fetchResult.getFetchedData(), fetchResult.getCursor());
				mNextFetchCourse = fetchResult.getCursor();
				mNextFetchMode = LogHubMode.AFTER;
			}
			sampleLogError(result);

			// if no error happen, fetch the data directly ,other wise don't
			// fetch data this time
			if (result == null || result.getException() == null) {
				LogHubFetchTask task = new LogHubFetchTask(mLogHubClient,
						mProject, mLogStream, mShardId, mNextFetchCourse,
						mNextFetchMode);
				mFetchDataFeture = mExecutorService.submit(task);
			} else {
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
		if (future != null && future.isDone()) {
			try {
				return future.get();
			} catch (InterruptedException e) {

			} catch (ExecutionException e) {
			}
		}
		return null;

	}

	private void generateNextTask() {
		ITask nextTask = null;
		if (this.mCurStatus.equals(ConsumerStatus.INITIALIZING)) {
			nextTask = new InitializeTask(mProcessor, this.mLeaseManager,
					mLogHubClient, mProject, mLogStream, mShardId, mCursorPosition);
		} else if (this.mCurStatus.equals(ConsumerStatus.PROCESSING)) {
			if (mLastFetchedData != null) {
				mCheckPointTracker.setCursor(mLastFetchedData.mEndCursor);
				nextTask = new ProcessTask(mProcessor,
						mLastFetchedData.mFetchedData, mCheckPointTracker);
				mLastFetchedData = null;
			}
		} else if (this.mCurStatus.equals(ConsumerStatus.SHUTTING_DOWN)) {
			nextTask = new ShutDownTask(mProcessor, mCheckPointTracker);
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
		if (isShutdown()) {
			checkAndGenerateNextTask();
		}
	}

	public boolean isShutdown() {
		return mCurStatus.equals(ConsumerStatus.SHUTDOWN_COMPLETE);
	}

}
