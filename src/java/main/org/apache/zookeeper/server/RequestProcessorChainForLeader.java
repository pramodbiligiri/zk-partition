package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.CommitProcessor;

public class RequestProcessorChainForLeader extends RequestProcessorChain {

	private CommitProcessor commitProcessor;

	public RequestProcessorChainForLeader(PrepRequestProcessor firstProcessor,
			Startable... startables) {
		super(firstProcessor, startables);
	}

	public void setCommitProcessor(CommitProcessor commitProcessor) {
		this.commitProcessor = commitProcessor;
	}

	public CommitProcessor getCommitProcessor() {
		return commitProcessor;
	}
}
