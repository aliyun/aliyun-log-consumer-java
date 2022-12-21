package com.aliyun.openservices.loghub.client.metrics;


import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.PutLogsRequest;
import com.aliyun.openservices.loghub.client.metrics.kv.LogKey;
import com.aliyun.openservices.loghub.client.metrics.kv.LogValue;
import com.aliyun.openservices.loghub.client.metrics.kv.MetricKey;
import com.aliyun.openservices.loghub.client.metrics.kv.MetricType;
import com.aliyun.openservices.loghub.client.metrics.kv.MetricValue;
import com.aliyun.openservices.loghub.client.metrics.kv.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LogMetricPublisher {

	private static final Logger LOG = LoggerFactory.getLogger(LogMetricPublisher.class);

	private static final int DEFAULT_MINUTE_PERIOD = 1;
	private static final int PRESET_AMOUNT = 3;
	private static final int RETRY_COUNT = 5;
	private static final int MAX_GROUP_SIZE = 4000;
	private static final int CLEAR_PERIOD = 60 * 60 * 24 * 3;

	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
	private final ExecutorService executorService = Executors.newFixedThreadPool(1);

	private Map<LogKey, LogValue> slsLog = new ConcurrentHashMap<LogKey, LogValue>();
	private Map<MetricKey, MetricValue> slsMetric = new ConcurrentHashMap<MetricKey, MetricValue>();
	private Set<LogDimension> logDimensions = new HashSet<LogDimension>();
	private Set<MetricDimension> metricDimensions = new HashSet<MetricDimension>();
	private Map<MetricDimension, AtomicLong> metricDimsStat = new ConcurrentHashMap<MetricDimension, AtomicLong>();

	private Client client;
	private LogMetricConfig config;

	private long lastClearTime = 0;
	private long startTime = 0;
	private int minutePeriod = 1;
	private int delaySeconds = 5;

	private AtomicBoolean isShutDown = new AtomicBoolean(false);

	public LogMetricPublisher(LogMetricConfig config, Set<LogDimension> logDimensions, Set<MetricDimension> metricDimensions) {
		this(config, DEFAULT_MINUTE_PERIOD, logDimensions, metricDimensions);
	}

	public LogMetricPublisher(LogMetricConfig config, int minutePeriod, Set<LogDimension> logDimensions, Set<MetricDimension> metricDimensions) {
		if (config == null) {
			throw new NullPointerException("LogMetric Config Can't Be Null");
		}
		if (minutePeriod < 1) {
			throw new IllegalArgumentException("LogMetric minPeriod Must Be Greater or Equal than 1");
		}

		SimpleDateFormat dataFormat = new SimpleDateFormat("yyyyMMddHHmm");
		try {
			startTime = dataFormat.parse(dataFormat.format(new Date())).getTime() / 1000;
			delaySeconds = System.currentTimeMillis() / 1000 - startTime > delaySeconds ? 0 : delaySeconds;
		} catch (ParseException e) {
		}

		this.config = config;
		this.minutePeriod = minutePeriod;
		this.lastClearTime = startTime;

		long timeIndex = getCurrTimeIndex();
		if (logDimensions != null && !logDimensions.isEmpty()) {
			addLogDimensions(timeIndex, logDimensions);
		}

		if (metricDimensions != null && !metricDimensions.isEmpty()) {
			addMetricDimensions(timeIndex, metricDimensions);
		}

		this.client = new Client(config.getEndpoint(), config.getAccessKeyId(), config.getAccessKeySecret());

		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				long timeIndex = getCurrTimeIndex();
				clearExpireDimensions();
				publishLogMetric(timeIndex, true, true);
				presetLogBuckets(timeIndex, PRESET_AMOUNT);
				presetMetricBuckets(timeIndex, PRESET_AMOUNT);

			}
		}, minutePeriod * 60 + delaySeconds, minutePeriod * 60, TimeUnit.SECONDS);
	}

	public void addLog(String level, String code, String message) {
		long timeIndex = getCurrTimeIndex();
		long[] wTimes = getSpanOfTimeIndex(timeIndex);

		LogKey logKey = new LogKey(timeIndex, wTimes[0], wTimes[1], level, code);
		LogValue logValue = slsLog.get(logKey);
		if (logValue == null) {
			LOG.info("LogMetric LogValue Is Null,LogKey Is:{}", logKey.toString());
			addLogDimension(timeIndex, new LogDimension(level, code));
			logValue = slsLog.get(logKey);
		}
		if (logValue != null) {
			logValue.incrementCount();
			logValue.setMessage(message);
		}
	}

	public void addReadMetric(String project, String logstore, String consumerGroup, String consumer, String shard, MetricReadRecord metricReadRecord) {
		long timeIndex = getCurrTimeIndex();
		long[] wTimes = getSpanOfTimeIndex(timeIndex);

		MetricKey metricKey = new MetricKey(timeIndex, wTimes[0], wTimes[1], project, logstore, consumerGroup, consumer, shard, MetricType.SOURCE_METRIC);
		MetricValue metricValue = slsMetric.get(metricKey);
		if (metricValue == null) {
			LOG.info("LogMetric MetricValue Is Null,MetricKey Is:{}", metricKey.toString());
			addMetricDimension(timeIndex, new MetricDimension(project, logstore, consumerGroup, consumer, shard, MetricType.SOURCE_METRIC));
			metricValue = slsMetric.get(metricKey);
		}
		if (metricValue != null) {
			metricValue.setFetchedDelay(metricReadRecord.getFetchedDelay());
			metricValue.addFetchedBytes(metricReadRecord.getFetchedBytes());
			metricValue.addFetchedCount(metricReadRecord.getFetchedCount());
			metricValue.addFetchedMillis(metricReadRecord.getFetchedMillis());
		}
		metricDimsStat(project, logstore, consumerGroup, consumer, shard, MetricType.SOURCE_METRIC);
	}


	public void addWriteMetric(String project, String logstore, String consumerGroup, String consumer, String shard, MetricWriteRecord metricWriteRecord) {
		long timeIndex = getCurrTimeIndex();
		long[] wTimes = getSpanOfTimeIndex(timeIndex);

		MetricKey metricKey = new MetricKey(timeIndex, wTimes[0], wTimes[1], project, logstore, consumerGroup, consumer, shard, MetricType.SINK_METRIC);
		MetricValue metricValue = slsMetric.get(metricKey);
		if (metricValue == null) {
			LOG.info("LogMetric MetricValue Is Null,MetricKey Is:{}", metricKey.toString());
			addMetricDimension(timeIndex, new MetricDimension(project, logstore, consumerGroup, consumer, shard, MetricType.SINK_METRIC));
			metricValue = slsMetric.get(metricKey);
		}
		if (metricValue != null) {
			metricValue.addDelivered(metricWriteRecord.getDelivered());
			metricValue.addFailed(metricWriteRecord.getFailed());
			metricValue.addDropped(metricWriteRecord.getDropped());
			metricValue.setProcessedDelay(metricWriteRecord.getProcessedDelay());
			metricValue.addWriteCount(metricWriteRecord.getWriteCount());
			metricValue.addWriteBytes(metricWriteRecord.getWriteBytes());
			metricValue.addWriteMillis(metricWriteRecord.getWriteMillis());
		}
		metricDimsStat(project, logstore, consumerGroup, consumer, shard, MetricType.SINK_METRIC);

	}

	public void addLogDimension(LogDimension logDimension) {
		addLogDimension(getCurrTimeIndex(), logDimension);
	}

	public void addLogDimension(long timeIndex, LogDimension logDimension) {
		synchronized (this.logDimensions) {
			this.logDimensions.add(logDimension);
			presetLogBuckets(timeIndex, PRESET_AMOUNT);
		}
	}

	public void addLogDimensions(Set<LogDimension> logDimensions) {
		addLogDimensions(getCurrTimeIndex(), logDimensions);
	}

	public void addLogDimensions(long timeIndex, Set<LogDimension> logDimensions) {
		synchronized (this.logDimensions) {
			this.logDimensions.addAll(logDimensions);
			presetLogBuckets(timeIndex, PRESET_AMOUNT);
		}
	}

	public void addMetricDimension(MetricDimension metricDimension) {
		addMetricDimension(getCurrTimeIndex(), metricDimension);
	}

	public void addMetricDimension(long timeIndex, MetricDimension metricDimension) {
		synchronized (this.metricDimensions) {
			this.metricDimensions.add(metricDimension);
			this.metricDimsStat.putIfAbsent(metricDimension, new AtomicLong(0));
			presetMetricBuckets(timeIndex, PRESET_AMOUNT);
		}
	}

	public void addMetricDimensions(Set<MetricDimension> metricDimensions) {
		addMetricDimensions(getCurrTimeIndex(), metricDimensions);
	}

	public void addMetricDimensions(long timeIndex, Set<MetricDimension> metricDimensions) {
		synchronized (this.metricDimensions) {
			this.metricDimensions.addAll(metricDimensions);
			for (MetricDimension metricDimension : metricDimensions) {
				this.metricDimsStat.putIfAbsent(metricDimension, new AtomicLong(0));
			}
			presetMetricBuckets(timeIndex, PRESET_AMOUNT);
		}
	}

	public void removeMetricDimension(MetricDimension metricDimension) {
		synchronized (this.metricDimensions) {
			this.metricDimensions.remove(metricDimension);
			presetMetricBuckets(getCurrTimeIndex(), PRESET_AMOUNT);
		}
	}

	public void shutdown() {
		if (isShutDown.compareAndSet(false, true)) {
			scheduledExecutorService.shutdown();
			try {
				scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.warn(e.getMessage(), e);
			}

			executorService.shutdown();
			try {
				executorService.awaitTermination(60, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.warn(e.getMessage(), e);
			}

			publishLogMetric(false, false);
		}
	}

	private void publishLogMetric(boolean delay, boolean async) {
		this.publishLogMetric(getCurrTimeIndex(), delay, async);
	}

	private void publishLogMetric(long timeIndex, boolean delay, boolean async) {
		LOG.debug("LogMetric Current TimeIndex Is:{},Delay:{}", timeIndex, delay);

		List<LogItem> logGroup = new ArrayList<LogItem>();
		Iterator<Map.Entry<LogKey, LogValue>> logEntries = slsLog.entrySet().iterator();
		while (logEntries.hasNext()) {
			Map.Entry<LogKey, LogValue> logEntry = logEntries.next();
			if (logEntry.getKey().getTimeIndex() <= timeIndex - (delay ? 1 : 0)) {
				if (logEntry.getValue().getCount() > 0) {
					logGroup.add(createLogItem(logEntry.getKey(), logEntry.getValue()));
				}
				logEntries.remove();
				LOG.debug("LogMetric SlsLog Remove:{}", logEntry.getKey().toString());
			}
		}

		Iterator<Map.Entry<MetricKey, MetricValue>> metricEntries = slsMetric.entrySet().iterator();
		while (metricEntries.hasNext()) {
			Map.Entry<MetricKey, MetricValue> metricEntry = metricEntries.next();
			if (metricEntry.getKey().getTimeIndex() <= timeIndex - (delay ? 1 : 0)) {
				MetricType metricType = metricEntry.getKey().getMetricType();
				if (metricType == MetricType.SOURCE_METRIC) {
					logGroup.add(createMetricItem(MetricType.SOURCE_METRIC, metricEntry.getKey(), metricEntry.getValue()));
				} else if (metricType == MetricType.SINK_METRIC) {
					logGroup.add(createMetricItem(MetricType.SINK_METRIC, metricEntry.getKey(), metricEntry.getValue()));
				}
				metricEntries.remove();
				LOG.debug("LogMetric SlsMetric Remove:{}", metricEntry.getKey().toString());
			}
		}

		if (!logGroup.isEmpty()) {
			List<List<LogItem>> splits = split(logGroup);
			for (List<LogItem> split : splits) {
				final PutLogsRequest putReq = new PutLogsRequest(config.getProject(), config.getLogstore(), config.getTopic(), config.getSource(), split);
				putReq.SetTags(config.getTags());
				if (async) {
					executorService.execute(new Runnable() {
						@Override
						public void run() {
							retryPutLogs(client, putReq);
						}
					});
				} else {
					retryPutLogs(client, putReq);
				}
			}
		}
	}

	private long[] getSpanOfTimeIndex(long timeIndex) {
		long windowStartTime = timeIndex * (60 * minutePeriod) + startTime;
		long windowEndTime = windowStartTime + 60 * minutePeriod - 1;
		return new long[]{windowStartTime, windowEndTime};
	}

	private long getCurrTimeIndex() {
		return (System.currentTimeMillis() / 1000 - startTime) / (60 * minutePeriod);
	}


	private void presetLogBuckets(int amount) {
		long currTimeIndex = getCurrTimeIndex();
		this.presetLogBuckets(currTimeIndex, amount);
	}

	private void presetLogBuckets(long currTimeIndex, int amount) {
		for (int i = 0; i < amount; i++) {
			long timeIndex = currTimeIndex + i;
			long[] windowTimes = getSpanOfTimeIndex(timeIndex);
			Window window = new Window(timeIndex, windowTimes[0], windowTimes[1]);
			presetLogBucket(window);
		}
	}

	private void presetLogBucket(Window window) {
		synchronized (this.logDimensions) {
			if (logDimensions != null && !logDimensions.isEmpty()) {
				for (LogDimension dimension : logDimensions) {
					LogKey logKey = new LogKey();
					logKey.mixWindow(window);
					logKey.mixDimension(dimension);
					if (slsLog.putIfAbsent(logKey, new LogValue()) == null) {
						LOG.debug("LogMetric Preset LogKey:{}", logKey.toString());
					}
				}
			}
		}
	}

	private void presetMetricBuckets(int amount) {
		long currTimeIndex = getCurrTimeIndex();
		this.presetMetricBuckets(currTimeIndex, amount);
	}

	private void presetMetricBuckets(long currTimeIndex, int amount) {
		for (int i = 0; i < amount; i++) {
			long timeIndex = currTimeIndex + i;
			long[] windowTimes = getSpanOfTimeIndex(timeIndex);
			Window window = new Window(timeIndex, windowTimes[0], windowTimes[1]);
			presetMetricBucket(window);
		}
	}

	private void presetMetricBucket(Window window) {
		synchronized (this.metricDimensions) {
			if (metricDimensions != null && !metricDimensions.isEmpty()) {
				for (MetricDimension dimension : metricDimensions) {
					MetricKey metricKey = new MetricKey();
					metricKey.mixWindow(window);
					metricKey.mixDimension(dimension);
					if (slsMetric.putIfAbsent(metricKey, new MetricValue()) == null) {
						LOG.debug("LogMetric Preset MetricKey:{}", metricKey.toString());
					}
				}
			}
		}
	}

	private void metricDimsStat(String project, String logstore, String consumerGroup, String consumer, String shard, MetricType metricType) {
		MetricDimension metricDimension = new MetricDimension(project, logstore, consumerGroup, consumer, shard, metricType);
		AtomicLong amount = metricDimsStat.get(metricDimension);
		if (amount != null) {
			amount.incrementAndGet();
		}
	}

	private void clearExpireDimensions() {
		long now = System.currentTimeMillis() / 1000;
		if (now - lastClearTime > CLEAR_PERIOD) {
			Iterator<Map.Entry<MetricDimension, AtomicLong>> metricDimsStatEntries = metricDimsStat.entrySet().iterator();
			while (metricDimsStatEntries.hasNext()) {
				Map.Entry<MetricDimension, AtomicLong> metricDimsStatEntry = metricDimsStatEntries.next();
				if (metricDimsStatEntry.getValue().get() <= 0) {
					removeMetricDimension(metricDimsStatEntry.getKey());
					metricDimsStatEntries.remove();
					LOG.info("LogMetric Clear Dimension:{}", metricDimsStatEntry.getKey().toString());
				} else {
					metricDimsStatEntry.getValue().getAndSet(0);
				}
			}
			lastClearTime = now;
		}
	}

	private LogItem createLogItem(LogKey logKey, LogValue logValue) {
		LogItem logItem = new LogItem((int) logKey.getStartTime());
		logItem.PushBack("type", "log");
		logItem.PushBack("start", String.valueOf(logKey.getStartTime()));
		logItem.PushBack("end", String.valueOf(logKey.getEndTime()));

		logItem.PushBack("level", logKey.getLevel());
		logItem.PushBack("code", logKey.getCode());

		logItem.PushBack("count", String.valueOf(logValue.getCount()));
		logItem.PushBack("message", logValue.getMessage());

		return logItem;
	}

	private LogItem createMetricItem(MetricType metricType, MetricKey metricKey, MetricValue metricValue) {
		LogItem logItem = new LogItem((int) metricKey.getStartTime());
		logItem.PushBack("type", metricType.name().toLowerCase());
		logItem.PushBack("start", String.valueOf(metricKey.getStartTime()));
		logItem.PushBack("end", String.valueOf(metricKey.getEndTime()));

		logItem.PushBack("project", metricKey.getProject());
		logItem.PushBack("logstore", metricKey.getLogstore());
		logItem.PushBack("consumerGroup", metricKey.getConsumerGroup());
		logItem.PushBack("consumer", metricKey.getConsumer());

		if (metricType == MetricType.SOURCE_METRIC) {
			logItem.PushBack("shard", metricKey.getShard());
			logItem.PushBack("fetchedDelay", String.valueOf(metricValue.getFetchedDelay()));
			logItem.PushBack("fetchedBytes", String.valueOf(metricValue.getFetchedBytes()));
			logItem.PushBack("fetchedCount", String.valueOf(metricValue.getFetchedCount()));
			logItem.PushBack("fetchedMillis", String.valueOf(metricValue.getFetchedMillis()));
		} else if (metricType == MetricType.SINK_METRIC) {
			logItem.PushBack("delivered", String.valueOf(metricValue.getDelivered()));
			logItem.PushBack("dropped", String.valueOf(metricValue.getDropped()));
			logItem.PushBack("failed", String.valueOf(metricValue.getFailed()));
			logItem.PushBack("writeCount", String.valueOf(metricValue.getWriteCount()));
			logItem.PushBack("writeBytes", String.valueOf(metricValue.getWriteBytes()));
			logItem.PushBack("writeMillis", String.valueOf(metricValue.getWriteMillis()));
		}

		return logItem;
	}

	private void retryPutLogs(Client client, PutLogsRequest putReq) {
		int retryNum = 0;
		while (retryNum < RETRY_COUNT) {
			try {
				client.PutLogs(putReq);
				return;
			} catch (LogException e) {
				if (!"LogStoreNotExist".equals(e.GetErrorCode()) && !"ProjectNotExist".equals(e.GetErrorCode())) {
					LOG.error("LogMetric PutLogs Exception", e);
				}
				if (("RequestError".equals(e.GetErrorCode()) || "InternalServerError".equals(e.GetErrorCode())) && ++retryNum < RETRY_COUNT) {
					try {
						Thread.sleep(200);
						continue;
					} catch (InterruptedException ex) {
						LOG.error("LogMetric PutLogs InterruptedException", ex);
					}
				}
				return;
			}
		}
	}

	private static <T> List<List<T>> split(List<T> logGroup) {
		List<List<T>> splits = new ArrayList<List<T>>();
		if (logGroup.size() > MAX_GROUP_SIZE) {
			for (int i = 0; i < logGroup.size(); i += MAX_GROUP_SIZE) {
				int end = i + MAX_GROUP_SIZE < logGroup.size() ? i + MAX_GROUP_SIZE : logGroup.size();
				splits.add(logGroup.subList(i, end));
			}
		} else {
			splits.add(logGroup);
		}
		return splits;
	}
}