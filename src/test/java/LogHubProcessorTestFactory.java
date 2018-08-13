import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;


public class LogHubProcessorTestFactory implements ILogHubProcessorFactory {

	@Override
	public ILogHubProcessor generatorProcessor() {
		return new LogHubTestProcessor();
	}

}
