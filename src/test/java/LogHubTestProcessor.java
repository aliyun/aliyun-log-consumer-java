import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;


public class LogHubTestProcessor implements ILogHubProcessor {
	private int mShard;
	private long mLastCheckTime = 0;
	@Override
	public void initialize(int shardId) {
		System.out.println("initialize shard: " + shardId);
		mShard = shardId;
	}

	@Override
	public String process(List<LogGroupData> logGroups,
			ILogHubCheckPointTracker checkPointTracker) 
	{
		long curTime = System.currentTimeMillis();
	    if (curTime - mLastCheckTime >  60 * 1000) {
	        try {
	            checkPointTracker.saveCheckPoint(true);
	        } catch (LogHubCheckPointException e) {
	            e.printStackTrace();
	        }
	        mLastCheckTime = curTime;
	    } else {
	        try {
	            checkPointTracker.saveCheckPoint(false);
	        } catch (LogHubCheckPointException e) {
	            e.printStackTrace();
	        }
	    }
		return null;
	}

	@Override
	public void shutdown(ILogHubCheckPointTracker checkPointTracker) 
	{
	}

}
