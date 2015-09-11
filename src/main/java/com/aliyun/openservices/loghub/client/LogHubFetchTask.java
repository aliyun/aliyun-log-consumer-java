package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

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
	private static final Logger logger = Logger.getLogger(LogHubFetchTask.class);

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

				logger.debug("shard id = " + mShardId + " cursor = " + mCursor
						+ " next cursor" + response.GetNextCursor() + " size:"
						+ String.valueOf(response.GetCount()));
				
				for (int i = 0 ; i < response.GetCount(); i++)
				{
					LogGroupData group = response.GetLogGroup(i);
					if (group != null)
					{
						fetchedData.add(group);
					}
				}
				
				String cursor = response.GetNextCursor();
				
				if (cursor.isEmpty()) {
					return new FetchTaskResult(fetchedData, mCursor);
				} else {
					return new FetchTaskResult(fetchedData, cursor);
				}
			} catch (Exception e) {
				exception = e;
			}
		
			// only retry if the first request get "SLSInvalidCursor" exception
			if (retry == 0
					&& exception instanceof SlsException
					&& ((SlsException) (exception)).GetErrorCode()
							.toLowerCase().indexOf("invalidcursor") != -1) {
				try {
					freshCursor();
				} catch (Exception e) {
					return new TaskResult(exception);
				}
				continue;
			} else {
				break;
			}
		}
		return new TaskResult(exception);
	}

	public void freshCursor() throws NumberFormatException, SlsException {
		GetCursorResponse response = mLoghubClient.GetCursor(mProject,
				mLogStream, Integer.parseInt(mShardId), CursorMode.END);
		mCursor = response.GetCursor();
	}
}
