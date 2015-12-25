import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.excpetions.LogHubClientWorkerException;

public class ClientWorkerTest {

	public static void main(String[] args) throws LogHubClientWorkerException,
			InterruptedException {
		LogHubConfig config1 = new LogHubConfig(
				"consumer_group_client_worker_test", "consumer_0",
				"cn-hangzhou-failover-intranet.sls.aliyuncs.com",
				"ali-cn-failover-sls-admin", "sls_operation_log",
				"94to3z418yupi6ikawqqd370", "DFk3ONbf81veUFpMg7FtY0BLB2w=",
				LogHubCursorPosition.BEGIN_CURSOR, 20 * 1000, false), config2 = new LogHubConfig(
				"consumer_group_client_worker_test", "consumer_1",
				"cn-hangzhou-failover-intranet.sls.aliyuncs.com",
				"ali-cn-failover-sls-admin", "sls_operation_log",
				"94to3z418yupi6ikawqqd370", "DFk3ONbf81veUFpMg7FtY0BLB2w=",
				LogHubCursorPosition.BEGIN_CURSOR, 20 * 1000, false), config3 = new LogHubConfig(
				"consumer_group_client_worker_test", "consumer_2",
				"cn-hangzhou-failover-intranet.sls.aliyuncs.com",
				"ali-cn-failover-sls-admin", "sls_operation_log",
				"94to3z418yupi6ikawqqd370", "DFk3ONbf81veUFpMg7FtY0BLB2w=",
				LogHubCursorPosition.BEGIN_CURSOR, 20 * 1000, false), config4 = new LogHubConfig(
				"consumer_group_client_worker_test", "consumer_3",
				"cn-hangzhou-failover-intranet.sls.aliyuncs.com",
				"ali-cn-failover-sls-admin", "sls_operation_log",
				"94to3z418yupi6ikawqqd370", "DFk3ONbf81veUFpMg7FtY0BLB2w=",
				LogHubCursorPosition.BEGIN_CURSOR, 20 * 1000, false), config5 = new LogHubConfig(
				"consumer_group_client_worker_test", "consumer_4",
				"cn-hangzhou-failover-intranet.sls.aliyuncs.com",
				"ali-cn-failover-sls-admin", "sls_operation_log",
				"94to3z418yupi6ikawqqd370", "DFk3ONbf81veUFpMg7FtY0BLB2w=",
				LogHubCursorPosition.BEGIN_CURSOR, 20 * 1000, false);

		ClientWorker worker1 = new ClientWorker(
				new LogHubProcessorTestFactory(), config1), worker2 = new ClientWorker(
				new LogHubProcessorTestFactory(), config2), worker3 = new ClientWorker(
				new LogHubProcessorTestFactory(), config3), worker4 = new ClientWorker(
				new LogHubProcessorTestFactory(), config4), worker5 = new ClientWorker(
				new LogHubProcessorTestFactory(), config5);

		Thread thread1 = new Thread(worker1), thread2 = new Thread(worker2), thread3 = new Thread(
				worker3), thread4 = new Thread(worker4), thread5 = new Thread(worker5);
		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();
		thread5.start();

		//*
		Thread.sleep(60 * 60 * 1000);

		worker1.shutdown();

		
		//*/
		Thread.sleep(12 * 60 * 60 * 1000);

		worker2.shutdown();
		worker3.shutdown();
		worker4.shutdown();
		worker5.shutdown();

		Thread.sleep(60 * 1000);
	}

}
