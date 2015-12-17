import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.excpetions.LogHubClientWorkerException;


public class ClientWorkerTest {

	public static void main(String[] args) throws LogHubClientWorkerException, InterruptedException 
	{
		LogHubConfig config1 = new LogHubConfig(
				"consumer_group_client_worker_test",
				"consumer_0",
				"cn-hangzhou-failover-intranet.sls.aliyuncs.com", 
				"ali-cn-failover-sls-admin",
				"sls_fastcgi_log",
				"94to3z418yupi6ikawqqd370",
				"DFk3ONbf81veUFpMg7FtY0BLB2w=",
				LogHubCursorPosition.BEGIN_CURSOR,
				20 * 1000,
				true
				),
				config2 = new LogHubConfig(
						"consumer_group_client_worker_test",
						"consumer_1",
						"cn-hangzhou-failover-intranet.sls.aliyuncs.com", 
						"ali-cn-failover-sls-admin",
						"sls_fastcgi_log",
						"94to3z418yupi6ikawqqd370",
						"DFk3ONbf81veUFpMg7FtY0BLB2w=",
						LogHubCursorPosition.BEGIN_CURSOR,
						20 * 1000,
						true
						),
				config3 = new LogHubConfig(
				"consumer_group_client_worker_test",
				"consumer_2",
				"cn-hangzhou-failover-intranet.sls.aliyuncs.com", 
				"ali-cn-failover-sls-admin",
				"sls_fastcgi_log",
				"94to3z418yupi6ikawqqd370",
				"DFk3ONbf81veUFpMg7FtY0BLB2w=",
				LogHubCursorPosition.BEGIN_CURSOR,
				20 * 1000,
				true
				);
				
		ClientWorker worker1 = new ClientWorker(new LogHubProcessorTestFactory(), config1),
				 worker2 = new ClientWorker(new LogHubProcessorTestFactory(), config2),
				 worker3 = new ClientWorker(new LogHubProcessorTestFactory(), config3);
		
		Thread thread1 = new Thread(worker1),
				thread2 = new Thread(worker2),
				thread3 = new Thread(worker3);
		thread1.start();
		thread2.start();
		thread3.start();
		
		Thread.sleep(5 * 60 * 1000);
		
		worker1.shutdown();
		
		Thread.sleep(5 * 60 * 1000);
		
		worker2.shutdown();
		worker3.shutdown();
		
		Thread.sleep(30 * 1000);
	}

}
