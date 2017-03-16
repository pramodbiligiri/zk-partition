package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.CommitProcessor;

public class RequestProcessorChainForFollower extends RequestProcessorChain {

	private CommitProcessor commitProcessor;
	private SyncRequestProcessor syncProcessor;

	public RequestProcessorChainForFollower(RequestProcessor firstProcessor,
			Startable... threads) {
		super(firstProcessor, threads);
	}

	public void setCommitProcessor(CommitProcessor commitProcessor) {
		this.commitProcessor = commitProcessor;
	}

	public void setSyncProcessor(SyncRequestProcessor syncProcessor) {
		this.syncProcessor = syncProcessor;
	}

	public CommitProcessor getCommitProcessor() {
		return commitProcessor;
	}

	public SyncRequestProcessor getSyncProcessor() {
		return syncProcessor;
	}
}