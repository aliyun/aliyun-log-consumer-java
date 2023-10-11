import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import org.junit.Test;

import java.util.Map;

public class ClientWorkerTest {
    static class EnvVar{
        public String accessKeyID;
        public String accessKeySecret;
        public String endpoint;
        public String project;
        public String logStore;
        private static boolean isEmpty(String str) {
            return str == null || str.isEmpty();
        }
        EnvVar() {
            Map<String, String> envVariables = System.getenv();
            this.accessKeyID = envVariables.get("LOG_TEST_ACCESS_KEY_ID");
            this.accessKeySecret = envVariables.get("LOG_TEST_ACCESS_KEY_SECRET");
            this.endpoint = envVariables.get("LOG_TEST_ENDPOINT");
            this.project = envVariables.get("LOG_TEST_PROJECT");
            this.logStore = envVariables.get("LOG_TEST_LOGSTORE");
            if (isEmpty(this.accessKeyID) || isEmpty(this.accessKeySecret) || isEmpty(this.endpoint)) {
                throw new RuntimeException("accessKeyId / accessKeySecret / endpoint must be set in environment variables");
            }
        }
    }
    private static final EnvVar env = new EnvVar();

    private static final String TEST_PROJECT = env.project;
    private static final String TEST_ENDPOINT = env.endpoint;
    private static final String TEST_LOGSTORE = env.logStore;
    private static final String ACCESS_KEY_ID = env.accessKeyID;
    private static final String ACCESS_KEY = env.accessKeySecret;



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
    @Test
    public void TestCredentialsProvider() throws LogHubClientWorkerException, InterruptedException {
        int n = 5;
        Thread[] threads = new Thread[n];
        ClientWorker[] workers = new ClientWorker[n];
        CredentialsProvider provider = new StaticCredentialsProvider(new DefaultCredentials(ACCESS_KEY_ID, ACCESS_KEY,""));

        for (int i = 0; i < n; i++) {
            LogHubConfig config = new LogHubConfig(
                    "consumer_group_client_worker_test", "consumer_" + i,
                    TEST_ENDPOINT, TEST_PROJECT, TEST_LOGSTORE,
                    provider,
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


    @Test
    public void TestCredentialsProvider2() throws LogHubClientWorkerException, InterruptedException {
        int n = 5;
        Thread[] threads = new Thread[n];
        ClientWorker[] workers = new ClientWorker[n];
        CredentialsProvider provider = new StaticCredentialsProvider(new DefaultCredentials(ACCESS_KEY_ID, ACCESS_KEY,""));

        int startTime = (int) (System.currentTimeMillis() / 1000);
        for (int i = 0; i < n; i++) {
            LogHubConfig config = new LogHubConfig(
                    "consumer_group_client_worker_test", "consumer_" + i,
                    TEST_ENDPOINT, TEST_PROJECT, TEST_LOGSTORE,
                    provider,
                    startTime);
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
