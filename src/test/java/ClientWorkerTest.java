import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;

public class ClientWorkerTest {

    private static final String TEST_PROJECT = "";
    private static final String TEST_ENDPOINT = "";
    private static final String TEST_LOGSTORE = "logstore-test";
    private static final String ACCESS_KEY_ID = "";
    private static final String ACCESS_KEY = "";

    public static void main(String[] args) throws LogHubClientWorkerException,
            InterruptedException {
        int n = 5;
        Thread[] threads = new Thread[n];
        ClientWorker[] workers = new ClientWorker[n];

        for (int i = 0; i < n; i++) {
            LogHubConfig config = new LogHubConfig(
                    "consumer_group_client_worker_test", "consumer_" + i,
                    TEST_ENDPOINT, TEST_PROJECT, TEST_LOGSTORE,
                    ACCESS_KEY_ID, ACCESS_KEY,
                    LogHubConfig.ConsumePosition.BEGIN_CURSOR);
            config.setHeartBeatIntervalMillis(20 * 1000);
            ClientWorker worker = new ClientWorker(new LogHubProcessorTestFactory(), config);
            threads[i] = new Thread(worker);
            workers[i] = worker;
        }
        for (int i = 0; i < n; i++) {
            threads[i].start();
        }

        //*
        Thread.sleep(60 * 1000);

        for (int i = 0; i < n; i++) {
            workers[i].shutdown();
        }

        Thread.sleep(60 * 1000);
    }

}
