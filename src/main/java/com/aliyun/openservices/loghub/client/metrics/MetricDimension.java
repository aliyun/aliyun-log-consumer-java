package com.aliyun.openservices.loghub.client.metrics;

import com.aliyun.openservices.loghub.client.metrics.kv.MetricType;

public class MetricDimension {

	private String project;
	private String logstore;
	private String consumerGroup;
	private String consumer;
	private String shard;
	private MetricType metricType;

	public MetricDimension(String project, String logstore, String consumerGroup, String consumer, String shard,MetricType metricType) {
		this.project = project;
		this.logstore = logstore;
		this.consumerGroup = consumerGroup;
		this.consumer = consumer;
		this.shard = shard;
		this.metricType = metricType;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public String getLogstore() {
		return logstore;
	}

	public void setLogstore(String logstore) {
		this.logstore = logstore;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getConsumer() {
		return consumer;
	}

	public void setConsumer(final String consumer) {
		this.consumer = consumer;
	}

	public String getShard() {
		return shard;
	}

	public void setShard(final String shard) {
		this.shard = shard;
	}

	public MetricType getMetricType() {
		return metricType;
	}

	public void setMetricType(final MetricType metricType) {
		this.metricType = metricType;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final MetricDimension that = (MetricDimension) o;

		if (!project.equals(that.project)) {
			return false;
		}
		if (!logstore.equals(that.logstore)) {
			return false;
		}
		if (!consumerGroup.equals(that.consumerGroup)) {
			return false;
		}
		if (!consumer.equals(that.consumer)) {
			return false;
		}
		if (!shard.equals(that.shard)) {
			return false;
		}
		return metricType == that.metricType;
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
		return "MetricDimension{" + "project='" + project + '\'' + ", logstore='" + logstore + '\'' + ", consumerGroup='" + consumerGroup + '\'' + ", consumer='" + consumer + '\'' + ", shard='" + shard + '\'' + ", metricType=" + metricType + '}';
	}
}
