package com.aliyun.openservices.loghub.client.metrics.kv;


import com.aliyun.openservices.loghub.client.metrics.MetricDimension;

public class MetricKey extends Window {

	private String project;
	private String logstore;
	private String consumerGroup;
	private String consumer;
	private String shard;
	private MetricType metricType;

	public MetricKey() {

	}

	public MetricKey(long timeIndex, long startTime, long endTime, String project, String logstore, String consumerGroup, String customer, String shard, MetricType metricType) {
		this.timeIndex = timeIndex;
		this.startTime = startTime;
		this.endTime = endTime;
		this.project = project;
		this.logstore = logstore;
		this.consumerGroup = consumerGroup;
		this.consumer = customer;
		this.shard = shard;
		this.metricType = metricType;
	}

	public String getProject() {
		return project;
	}

	public String getLogstore() {
		return logstore;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public String getConsumer() {
		return consumer;
	}

	public String getShard() {
		return shard;
	}

	public MetricType getMetricType() {
		return metricType;
	}

	public void mixDimension(MetricDimension metricDimension) {
		this.project = metricDimension.getProject();
		this.logstore = metricDimension.getLogstore();
		this.consumerGroup = metricDimension.getConsumerGroup();
		this.consumer = metricDimension.getConsumer();
		this.shard = metricDimension.getShard();
		this.metricType = metricDimension.getMetricType();
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final MetricKey metricKey = (MetricKey) o;

		if (!project.equals(metricKey.project)) {
			return false;
		}
		if (!logstore.equals(metricKey.logstore)) {
			return false;
		}
		if (!consumerGroup.equals(metricKey.consumerGroup)) {
			return false;
		}
		if (!consumer.equals(metricKey.consumer)) {
			return false;
		}
		if (!shard.equals(metricKey.shard)) {
			return false;
		}
		return metricType == metricKey.metricType;
	}

	@Override
	public int hashCode() {
		int result = project.hashCode();
		result = 31 * result + logstore.hashCode();
		result = 31 * result + consumerGroup.hashCode();
		result = 31 * result + consumer.hashCode();
		result = 31 * result + shard.hashCode();
		result = 31 * result + metricType.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "MetricKey{" + "project='" + project + '\'' + ", logstore='" + logstore + '\'' + ", consumerGroup='" + consumerGroup + '\'' + ", consumer='" + consumer + '\'' + ", shard='" + shard + '\'' + ", metricType=" + metricType + '}';
	}
}
