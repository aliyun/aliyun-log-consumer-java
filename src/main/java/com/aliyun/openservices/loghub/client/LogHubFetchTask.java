package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.openservices.loghub.LogHubClient;
import com.aliyun.openservices.loghub.common.LogGroup;
import com.aliyun.openservices.loghub.common.LogHubModes.LogHubMode;
import com.aliyun.openservices.loghub.exception.LogHubClientException;
import com.aliyun.openservices.loghub.exception.LogHubException;
import com.aliyun.openservices.loghub.response.GetCursorResponse;
import com.aliyun.openservices.loghub.response.GetLogDataResponse;

public class LogHubFetchTask implements ITask {
	private LogHubClient mLoghubClient;

	private String mProject;
	private String mLogStream;
	private String mShardId;
	private String mCursor;
	private LogHubMode mMode;
	private final int MAX_FETCH_LOGGROUP_SIZE = 1000;

	public LogHubFetchTask(LogHubClient client, String project,
			String logStream, String shardId, String cursor, LogHubMode mode) {
		mLoghubClient = client;
		mProject = project;
		mLogStream = logStream;
		mShardId = shardId;
		mCursor = cursor;
		mMode = mode;
	}

	public TaskResult call() {
		Exception exception = null;
		for (int retry = 0 ; retry < 2 ; retry++)
		{
			try {
				GetLogDataResponse response = mLoghubClient.getLogData(mProject, mLogStream,
						Integer.parseInt(mShardId), MAX_FETCH_LOGGROUP_SIZE,
						mMode, mCursor);
				List<LogGroup> fetchedData = new ArrayList<LogGroup>();
				
				for (int i = 0 ; i < response.getCount(); i++)
				{
					LogGroup group = response.getLogGroup(i);
					if (group != null)
					{
						fetchedData.add(group);
					}
				}
				
				String cursor = response.getLastCursor();
/*				System.out.println("fetch shard_id = " + mShardId
						+ ", fetech result : " + String.valueOf(fetchedData.size())
						+ ";get cursor:" + mCursor + ";return cursor:" + cursor);*/
				if (cursor.isEmpty()) {
					return new FetchTaskResult(fetchedData, mCursor);
				} else {
					return new FetchTaskResult(fetchedData, cursor);
				}
			} catch (Exception e) {
				exception = e;
			}
			
			// only retry if the first request get "InvalidCursor" exception
			if(retry == 0 && exception instanceof LogHubException && 
					((LogHubException)(exception)).getErrorCode().equals("InvalidCursor"))
			{ 
				try {
					freshCursor();
				} catch (Exception e) {
					return new TaskResult(exception);
				}
				continue;
			}
			else
			{
				break;
			}
		}
		return new TaskResult(exception);
	}
	public void freshCursor() throws NumberFormatException, LogHubException, LogHubClientException
	{
		GetCursorResponse response = mLoghubClient.getCursor(mProject,
				mLogStream, Integer.parseInt(mShardId), LogHubMode.BEGIN);
		mCursor = response.getCursor();
		mMode = LogHubMode.AT;
	}
}
