package org.apache.zookeeper.server.quorum;


public class StateSummary {
	private long currentEpoch;
	private long lastZxid;
	public StateSummary(long currentEpoch, long lastZxid) {
		this.currentEpoch = currentEpoch;
		this.lastZxid = lastZxid;
	}
	
	public long getCurrentEpoch() {
		return currentEpoch;
	}
	
	public long getLastZxid() {
		return lastZxid;
	}
	
	public boolean isMoreRecentThan(StateSummary ss) {
		return (currentEpoch > ss.currentEpoch) || (currentEpoch == ss.currentEpoch && lastZxid > ss.lastZxid);
	}
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof StateSummary)) {
			return false;
		}
		StateSummary ss = (StateSummary)obj;
		return currentEpoch == ss.currentEpoch && lastZxid == ss.lastZxid;
	}
	
	@Override
	public int hashCode() {
		return (int)(currentEpoch ^ lastZxid);
	}
}
