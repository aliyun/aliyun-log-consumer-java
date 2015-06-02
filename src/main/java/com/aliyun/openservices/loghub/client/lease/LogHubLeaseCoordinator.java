package com.aliyun.openservices.loghub.client.lease;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.client.LogHubClientAdapter;
import com.aliyun.openservices.loghub.client.lease.impl.LogHubLeaseRebalancer;
import com.aliyun.openservices.loghub.client.lease.impl.LogHubLeaseRenewer;

public class LogHubLeaseCoordinator {

	private static final long STOP_WAIT_TIME_MILLIS = 2000L;
	private static final long ESPILON_TIME_MILLS = 50;

	private final ILogHubLeaseRenewer mLeaseRenewer;
	private final ILogHubLeaseRebalancer mLeaseRebalancer;
	private final long mRenewerIntervalMillis;
	private final long mRebalacneIntervalMillis;

	private ScheduledExecutorService threadpool;
	protected Logger logger = Logger.getLogger(this.getClass());
	private boolean running = false;

	public LogHubLeaseCoordinator(LogHubClientAdapter clientAdpter,
			ILogHubLeaseManager leaseManager, String instanceName,
			long leaseDurationMillis) {
		this.mLeaseRebalancer = new LogHubLeaseRebalancer(clientAdpter,
				leaseManager, instanceName, leaseDurationMillis);
		this.mLeaseRenewer = new LogHubLeaseRenewer(leaseManager, instanceName,
				leaseDurationMillis);
		if (leaseDurationMillis > ESPILON_TIME_MILLS) {
			this.mRenewerIntervalMillis = (leaseDurationMillis - ESPILON_TIME_MILLS) / 2;
		} else {
			this.mRenewerIntervalMillis = leaseDurationMillis / 2;
		}
		this.mRebalacneIntervalMillis = (leaseDurationMillis + ESPILON_TIME_MILLS) * 2;
	}

	private class RebalancerRunnable implements Runnable {

		public void run() {
			try {
				runRebalance();
			} catch (Throwable t) {

			}
		}

	}

	private class RenewerRunnable implements Runnable {

		public void run() {
			try {
				runRenewer();
			}
			catch (Throwable t) {

			}
		}

	}


	public void start() {
		mLeaseRebalancer.initialize();
		mLeaseRenewer.initialize();

		threadpool = Executors.newScheduledThreadPool(2);

		threadpool.scheduleWithFixedDelay(new RebalancerRunnable(), 0L,
				mRebalacneIntervalMillis, TimeUnit.MILLISECONDS);
		
		threadpool.scheduleAtFixedRate(new RenewerRunnable(), 0L,
				mRenewerIntervalMillis, TimeUnit.MILLISECONDS);
		
		running = true;
	}


	protected void runRebalance() {
		try {
			List<LogHubLease> takenLeases = mLeaseRebalancer.takeLeases();
			mLeaseRenewer.addLeasesToRenew(takenLeases);
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("Failed to take lease because of exception:"
					+ e.getMessage());

		} finally {

		}
	}

	protected void runRenewer() {


		try {
			mLeaseRenewer.renewLeases();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
		}
	}

	/**
	 * @return currently held leases
	 */
	public Map<String, LogHubLease> getAllHeldLease() {
		return this.mLeaseRenewer.getHeldLeases();
	}

	/**
	 * @param leaseKey
	 *            lease key to fetch currently held lease for
	 * 
	 * @return deep copy of currently held Lease for given key, or null if we
	 *         don't hold the lease for that key
	 */
	public LogHubLease getHeldLeases(String leaseKey) {
		return this.mLeaseRenewer.getHeldLeases(leaseKey);
	}

	/**
	 * Stops background threads.
	 */
	public void stop() {
		if (threadpool != null) {
			threadpool.shutdown();
			try {
				if (threadpool.awaitTermination(STOP_WAIT_TIME_MILLIS,
						TimeUnit.MILLISECONDS)) {
				} else {
					threadpool.shutdownNow();

				}
			} catch (InterruptedException e) {

			}
		}
		this.mLeaseRenewer.clearHeldLeases();
		running = false;
	}

	/**
	 * @return true if this LeaseCoordinator is running
	 */
	public boolean isRunning() {
		return running;
	}

}
