package com.aliyun.openservices.loghub.client.metrics;


import com.aliyun.openservices.log.common.TagContent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogMetricConfig {

	private String endpoint;
	private String accessKeyId;
	private String accessKeySecret;
	private String project;
	private String logstore;
	private String topic;
	private String source;

	List<TagContent> tags = new ArrayList<TagContent>();

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(final String endpoint) {
		this.endpoint = endpoint;
	}

	public String getAccessKeyId() {
		return accessKeyId;
	}

	public void setAccessKeyId(final String accessKeyId) {
		this.accessKeyId = accessKeyId;
	}

	public String getAccessKeySecret() {
		return accessKeySecret;
	}

	public void setAccessKeySecret(final String accessKeySecret) {
		this.accessKeySecret = accessKeySecret;
	}

	public String getProject() {
		return project;
	}

	public void setProject(final String project) {
		this.project = project;
	}

	public String getLogstore() {
		return logstore;
	}

	public void setLogstore(final String logstore) {
		this.logstore = logstore;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(final String topic) {
		this.topic = topic;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public List<TagContent> getTags() {
		return tags;
	}

	public void setTags(final Map<String, String> tagKvs) {
		tags.clear();
		if (tagKvs != null && tagKvs.size() > 0) {
			for (Map.Entry<String, String> entry : tagKvs.entrySet()) {
				tags.add(new TagContent(entry.getKey(), entry.getValue()));
			}
		}
	}
}
