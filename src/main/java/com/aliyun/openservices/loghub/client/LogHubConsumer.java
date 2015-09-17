package com.aliyun.openservices.loghub.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.aliyun.openservices.sls.SLSClient;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;

public class LogHubConsumer {
	enum ConsumerStatus {
		INITIALIZING, PROCESSING, SHUTTING_DOWN, SHUTDOWN_COMPLETE
	}

	private String mShardId;
	private String mInstanceName;
	private SLSClient mLogHubClient;
	private String mProject;
	private String mLogStream;
	private DefaultLogHubCHeckPointTracker mCheckPointTracker;
	private ILogHubLeaseManager mLeaseManager;
	private ILogHubProcessor mProcessor;
	private LogHubCursorPosition mCursorPosition;
	private int mCursorStartTime = 0;
	
	private ConsumerStatus mCurStatus = ConsumerStatus.INITIALIZING;

	private ITask mCurrentTask;
	private Future<TaskResult> mTaskFuture;
	private Future<TaskResult> mFetchDataFeture;

	private ExecutorService mExecutorService;
	private String mNextFetchCourse;
	private boolean mShutDown = false;

	private FetchedLogGroup mLastFetchedData;
	
	private static final Logger logger = Logger.getLogger(LogHubConsumer.class);
	private long mLastLogErrorTime = 0;

	public LogHubConsumer(SLSClient loghubClient, String project,
			String logStream, String shardId, String instanceName,
			ILogHubLeaseManager leaseManager, ILogHubProcessor processor,
			ExecutorService executorService,  LogHubCursorPosition cursorPosition, int cursorStartTime) {
		mLogHubClient = loghubClient;
		mProject = project;
		mLogStream = logStream;
		mShardId = shardId;
		mInstanceName = instanceName;
		mCursorPosition = cursorPosition;
		mCursorStartTime = cursorStartTime;
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
					mNextFetchCourse = initResult.getCursor();
					mCheckPointTracker.setInMemoryCheckPoint(mNextFetchCourse);
					if(initResult.isCursorPersistent())
					{
						mCheckPointTracker.setInPeristentCheckPoint(mNextFetchCourse);
					}
				}
				else if (result instanceof ProcessTaskResult)
				{
					ProcessTaskResult process_task_result = (ProcessTaskResult)(result);
					String roll_back_checkpoint = process_task_result.getRollBackCheckpoint();
					if (roll_back_checkpoint != "" && roll_back_checkpoint.isEmpty() == false)
					{
						mLastFetchedData = null;
						CancelCurrentFetch();
						mNextFetchCourse = roll_back_checkpoint;
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
			if (result != null && result.getException() == null) {
				FetchTaskResult fetchResult = (FetchTaskResult) result;
				mLastFetchedData = new FetchedLogGroup(mShardId,
						fetchResult.getFetchedData(), fetchResult.getCursor());
				mNextFetchCourse = fetchResult.getCursor();
			}
			sampleLogError(result);

			// if no error happen, fetch the data directly ,other wise don't
			// fetch data this time
			if (result == null || result.getException() == null) {
				LogHubFetchTask task = new LogHubFetchTask(mLogHubClient,
						mProject, mLogStream, mShardId, mNextFetchCourse);
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
			nextTask = new InitializeTask(mProcessor, this.mLeaseManager,
					mLogHubClient, mProject, mLogStream, mShardId, mCursorPosition , mCursorStartTime);
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
