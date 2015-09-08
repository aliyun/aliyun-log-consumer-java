package com.aliyun.openservices.loghub.client.lease.impl;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.client.config.LogHubClientDbConfig;
import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;

public class MySqlLogHubLeaseManager implements ILogHubLeaseManager {
	
	private String mConsumeGroupName;
	private String mWorkerInstanceName;
	private String mSignature;
	private LogHubClientDbConfig mDbConfig;
	private String mMysqlUrl;
	private static final Logger logger = Logger.getLogger(MySqlLogHubLeaseManager.class);
	public MySqlLogHubLeaseManager(LogHubClientDbConfig  dbConfig)
	{
		mDbConfig = dbConfig;
		
		mMysqlUrl = "jdbc:mysql://" + mDbConfig.getDbHost() + ":" + mDbConfig.getDbPort() + "/" + mDbConfig.getDbName()
				+ "?connectTimeout=5000&socketTimeout=5000";
	}
	
	private String GetMd5Value(String body) {
		try {
			byte[] bytes = body.getBytes("utf-8");
			MessageDigest md;
			md = MessageDigest.getInstance("MD5");
			String res = new BigInteger(1, md.digest(bytes)).toString(16)
					.toUpperCase();

			StringBuilder zeros = new StringBuilder();
			for (int i = 0; i + res.length() < 32; i++) {
				zeros.append("0");
			}
			return zeros.toString() + res;
		} catch (NoSuchAlgorithmException e) {
			return "";
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}
	
	public boolean Initilize(String consumerGroupName,
			String workerInstanceName, String project, String logstore)
			throws LogHubLeaseException {
		this.mConsumeGroupName = consumerGroupName;
		this.mWorkerInstanceName = workerInstanceName;
		mSignature = GetMd5Value(project + "#" + logstore);
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			throw new LogHubLeaseException("Failed to load mysql jdbc driver", e1);
		}
		return creaseLeaseTable() &&  createWorkerTable() && registerWorker();
	}
	

	private boolean creaseLeaseTable() throws LogHubLeaseException {
		String table_sql = "Create Table if not exists " + mDbConfig.getLeaseTableName()
				+ "( consume_group varchar(64),"
				+ "logstream_sig varchar(64),"
				+ "shard_id varchar(64)," 
				+ "lease_id int(20),"
				+ "lease_owner varchar(64)," 
				+ "consumer_owner varchar(64),"
				+ "update_time datetime,"
				+ "checkpoint text,"
				+ "PRIMARY KEY(`consume_group`,`logstream_sig`, `shard_id`))"
				+ "ENGINE = InnoDB DEFAULT CHARSET=utf8;";
		return createTable(table_sql);
	}

	private boolean createWorkerTable() throws LogHubLeaseException{
		String table_sql = "Create Table if not exists " + mDbConfig.getWorkerTableName()
				+ "( consume_group varchar(64),"
				+ "logstream_sig varchar(64),"
				+ "instance_name varchar(64)," 
				+ "update_time datetime,"
				+ "PRIMARY KEY(`consume_group`, `logstream_sig`, `instance_name`)) "
				+ "ENGINE = InnoDB DEFAULT CHARSET=utf8;";
		return createTable(table_sql);
	}

	private boolean createTable(String table_sql) throws LogHubLeaseException {
		return updateQuery(table_sql, null) != -1;
	}
	
	private boolean registerWorker() throws LogHubLeaseException {
		String query = "insert into " + mDbConfig.getWorkerTableName()
				+ "(consume_group, logstream_sig, instance_name, update_time) values ('"
				+ mConsumeGroupName + "', '" + mSignature + "','" + mWorkerInstanceName
				+ "', now()) on duplicate key update update_time = now()";
		return updateQuery(query, null) != -1;
	}
	
	private int updateQuery(String query, Connection mysql_con)
			throws LogHubLeaseException {
		boolean new_create_con = false;
		if (mysql_con == null) {
			mysql_con = getConnection();
			new_create_con = true;
		}
		Statement state = null;
		try {
			mysql_con.setAutoCommit(false);
			state = mysql_con.createStatement();

			int res = state.executeUpdate(query);
			mysql_con.commit();

			return res;
		} catch (SQLException e) {
			try {
				mysql_con.rollback();
			} catch (SQLException e1) {
			}
			throw new LogHubLeaseException("Failed to execute update sql:"
					+ query, e);
		} finally {
			closeObj(state);
			if (new_create_con) {
				closeObj(mysql_con);
			}
		}
	}
	

	private Connection getConnection() throws LogHubLeaseException {
		try {
			return DriverManager.getConnection(mMysqlUrl,
					mDbConfig.getDbUser(), mDbConfig.getDbPassword());
		} catch (SQLException e) {
			throw new LogHubLeaseException("Failed to create mysql connection",
					e);
		}
	}


	private List<Map<String, String>> selectQuery(String query,
			List<String> column_list) throws LogHubLeaseException {
		List<Map<String, String>> return_val = new ArrayList<Map<String, String>>();
		Connection mysql_con = getConnection();
		Statement state = null;
		ResultSet res = null;

		try {
			state = mysql_con.createStatement();
			res = state.executeQuery(query);

			while (res.next()) {
				Map<String, String> values = new HashMap<String, String>();
				for (String key : column_list) {
					values.put(key, res.getString(key));
				}
				return_val.add(values);
			}

		} catch (SQLException e) {
			throw new LogHubLeaseException("Failed to select data from mysql",
					e);

		} finally {
			closeObj(res);
			closeObj(state);
			closeObj(mysql_con);
		}
		return return_val;

	}

	private void closeObj(AutoCloseable obj) {
		if (obj != null) {
			try {
				obj.close();
			} catch (Exception e) {
			}
		}
	}

	public List<String> listNewCreatedInstance() throws LogHubLeaseException {
		String sql = "select instance_name from "
				+ mDbConfig.getWorkerTableName() + " where  consume_group = "
				+ "'" + mConsumeGroupName + "' and logstream_sig ='" + mSignature
				+ "' and update_time > date_sub(now() , interval 60 second)";
		List<String> column_list = new ArrayList<String>();
		column_list.add("instance_name");
		List<Map<String, String>> query_res = selectQuery(sql, column_list);

		List<String> res = new ArrayList<String>();
		for (Map<String, String> row : query_res) {
			res.add(row.get("instance_name"));
		}
		return res;
	}

	public List<LogHubLease> listLeases() throws LogHubLeaseException {
		String sql = "select shard_id, lease_id, lease_owner, consumer_owner from "
				+ mDbConfig.getLeaseTableName()
				+ " where  consume_group = '"
				+ mConsumeGroupName + "' and logstream_sig ='" + mSignature + "'";
		List<String> column_list = new ArrayList<String>();
		column_list.add("shard_id");
		column_list.add("lease_id");
		column_list.add("lease_owner");
		column_list.add("consumer_owner");
		List<Map<String, String>> query_res = selectQuery(sql, column_list);
		List<LogHubLease> res = new ArrayList<LogHubLease>();
		for (Map<String, String> row : query_res) {
			res.add(new LogHubLease(row.get("shard_id"),
					row.get("lease_owner"), row.get("consumer_owner"), Long
							.parseLong(row.get("lease_id"))));
		}
		return res;
	}
	
	
	public boolean createLeaseForShards(List<String> shards_list) throws LogHubLeaseException{
		if (shards_list.isEmpty()) {
			return true;
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into " + mDbConfig.getLeaseTableName()
				+ "(consume_group , logstream_sig, shard_id, lease_id)  values");
		boolean first = true;
		for (String shard_id : shards_list) {
			if (first) {
				first = false;
			} else {
				sb.append(",");
			}
			sb.append("('" + mConsumeGroupName + "','" + mSignature + "','" + shard_id + "', 0)");
		}
		return updateQuery(sb.toString(), null) == shards_list.size();
	}


	public boolean renewLease(LogHubLease lease, boolean update_consumer) throws LogHubLeaseException{
		String query = "update " + mDbConfig.getLeaseTableName()
				+ " set update_time = now(), lease_id = " + String.valueOf(lease.getLeaseId() + 1);
		if (update_consumer) {
			query += " , consumer_owner = '" + lease.getLeaseOwner() + "'";

		}
		query += " where consume_group = '" + mConsumeGroupName
				+ "' and logstream_sig = '" + mSignature
				+ "' and shard_id = '" + lease.getLeaseKey()
				+ "' and lease_id = " + lease.getLeaseId();
		if (updateQuery(query, null) == 1) {
			if (update_consumer) {
				lease.makeConsumerHoldLease();
			}
			lease.setLeaseId(lease.getLeaseId() + 1);
			return true;
		}
		return false;
	}
	
	public void batchRenewLease(Map<String, LogHubLease> leases,
			Set<String> toRenewConsumerShards, Set<String> renewSuccessShards)
			throws LogHubLeaseException {
		if(leases.isEmpty())
		{
			return;
		}
		Connection con = getConnection();

		for (Map.Entry<String, LogHubLease> entry : leases.entrySet()) {

			String shardId = entry.getKey();
			LogHubLease lease = entry.getValue();

			String query = "update " + mDbConfig.getLeaseTableName()
					+ " set update_time = now(), lease_id = "
					+ String.valueOf(lease.getLeaseId() + 1);
			boolean update_consumer = false;
			if (toRenewConsumerShards.contains(shardId)) {
				query += " , consumer_owner = '" + lease.getLeaseOwner() + "'";
				update_consumer = true;
			}
			query += " where consume_group = '" + mConsumeGroupName
					+ "' and logstream_sig = '" + mSignature
					+ "' and shard_id = '" + lease.getLeaseKey()
					+ "' and lease_id = " + lease.getLeaseId();

			try {
				if (updateQuery(query, null) == 1) {
					lease.setLeaseId(lease.getLeaseId() + 1);
					if (update_consumer) {
						lease.makeConsumerHoldLease();
					}
					renewSuccessShards.add(shardId);
				}
			} catch (LogHubLeaseException e) {
				logger.warn(e);
			}
		}
		closeObj(con);
	}

	public boolean takeLease(LogHubLease lease, String leaseOnwer,
			String leaseConsumer) throws LogHubLeaseException {

		String query = "update " + mDbConfig.getLeaseTableName()
				+ " set update_time = now(),lease_id = " + String.valueOf(lease.getLeaseId() + 1)
				+ " , lease_owner = '" +  leaseOnwer + "'";
		if (leaseConsumer != null) {
			query += " , consumer_owner = '" + leaseConsumer + "'";
		}
		query += " where consume_group = '" + mConsumeGroupName
				+ "' and logstream_sig = '" + mSignature
				+ "' and shard_id = '" + lease.getLeaseKey()
				+ "' and lease_id = " + lease.getLeaseId();
		if (updateQuery(query, null) == 1) {
			lease.setLeaseOwner(leaseOnwer);
			lease.setLeaseId(lease.getLeaseId() + 1);
			if (leaseConsumer != null) {
				lease.setLeaseConsumer(leaseConsumer);
			}
			return true;
		}
		return false;
	}

	public boolean updateCheckPoint(String shardId, String checkPoint,
			String consumerOwner) throws LogHubLeaseException{
		String query = "update " + mDbConfig.getLeaseTableName()
				+ " set checkpoint = '" + checkPoint + "'"
				+ " , update_time = now()  where consume_group = '" + mConsumeGroupName
				+ "' and logstream_sig = '" + mSignature
				+ "' and shard_id = '" + shardId + "' and consumer_owner = '" + consumerOwner + "'";
		return updateQuery(query, null) == 1;
	}

	public String getCheckPoint(String shardId) throws LogHubLeaseException {
		String query = "select checkpoint from "
				+ mDbConfig.getLeaseTableName() + " where consume_group = '"
				+ mConsumeGroupName + "' and logstream_sig = '" + mSignature
				+ "' and shard_id = '" + shardId + "'";
		List<String> column_list = new ArrayList<String>();
		column_list.add("checkpoint");
		List<Map<String, String>> query_res = selectQuery(query, column_list);
		if (query_res.size() > 0) {
			return query_res.get(0).get("checkpoint");
		}
		return null;
	}


	
}
