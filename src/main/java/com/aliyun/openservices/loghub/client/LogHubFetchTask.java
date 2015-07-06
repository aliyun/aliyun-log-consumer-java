package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.openservices.sls.SLSClient;
import com.aliyun.openservices.sls.common.LogGroupData;
import com.aliyun.openservices.sls.common.SlsConsts.CursorMode;
import com.aliyun.openservices.sls.exception.SlsException;
import com.aliyun.openservices.sls.response.GetCursorResponse;
import com.aliyun.openservices.sls.response.BatchGetLogResponse;

public class LogHubFetchTask implements ITask {
	private SLSClient mLoghubClient;

	private String mProject;
	private String mLogStream;
	private String mShardId;
	private String mCursor;
	private final int MAX_FETCH_LOGGROUP_SIZE = 1000;

	public LogHubFetchTask(SLSClient client, String project,
			String logStream, String shardId, String cursor) {
		mLoghubClient = client;
		mProject = project;
		mLogStream = logStream;
		mShardId = shardId;
		mCursor = cursor;
	}

	public TaskResult call() {
		Exception exception = null;
		for (int retry = 0 ; retry < 2 ; retry++)
		{
			try {
				BatchGetLogResponse response = mLoghubClient.BatchGetLog(mProject, mLogStream,
						Integer.parseInt(mShardId), MAX_FETCH_LOGGROUP_SIZE, mCursor);
				List<LogGroupData> fetchedData = new ArrayList<LogGroupData>();
				
				for (int i = 0 ; i < response.GetCount(); i++)
				{
					LogGroupData group = response.GetLogGroup(i);
					if (group != null)
					{
						fetchedData.add(group);
					}
				}
				
				String cursor = response.GetNextCursor();
				
				if (fetchedData.size() > 0)
				{
					System.out.println("fetch shard_id = " + mShardId
						+ ", fetech result : " + String.valueOf(fetchedData.size())
						+ ";get cursor:" + mCursor + ";return cursor:" + cursor);
				}
				
				if (cursor.isEmpty()) {
					return new FetchTaskResult(fetchedData, mCursor);
				} else {
					return new FetchTaskResult(fetchedData, cursor);
				}
			} catch (Exception e) {
				exception = e;
			}
			
			// only retry if the first request get "InvalidCursor" exception
			if(retry == 0 && exception instanceof SlsException && 
					((SlsException)(exception)).GetErrorCode().equals("InvalidCursor"))
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
	public void freshCursor() throws NumberFormatException, SlsException
	{
		GetCursorResponse response = mLoghubClient.GetCursor(mProject,
				mLogStream, Integer.parseInt(mShardId), CursorMode.BEGIN);
		mCursor = response.GetCursor();
	}
}
