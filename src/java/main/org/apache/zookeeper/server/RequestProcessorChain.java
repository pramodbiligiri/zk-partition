package org.apache.zookeeper.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.quorum.CommitProcessor;
import org.apache.zookeeper.server.quorum.FollowerRequestProcessor;
import org.apache.zookeeper.server.quorum.FollowerZooKeeperServer;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.Learner;
import org.apache.zookeeper.server.quorum.ProposalRequestProcessor;
import org.apache.zookeeper.server.quorum.SendAckRequestProcessor;

public class RequestProcessorChain implements RequestProcessor {
	
	private final RequestProcessor firstProcessor;
	List<Startable> threads = new ArrayList<Startable>();
	
    final List<ChangeRecord> outstandingChanges = new ArrayList<ChangeRecord>();

	// this data structure must be accessed under the outstandingChanges lock
    final HashMap<String, ChangeRecord> outstandingChangesForPath =
        new HashMap<String, ChangeRecord>();
	
	public RequestProcessorChain(RequestProcessor firstProcessor, Startable ...threads){
		this.firstProcessor = firstProcessor;
		for(Startable thread : threads){
			this.threads.add(thread);
		}
	}
	
	public static RequestProcessorChain forDefault(ZooKeeperServer zks){
		FinalRequestProcessor finalProcessor = new FinalRequestProcessor(zks);
		SyncRequestProcessor syncProcessor = new SyncRequestProcessor(zks,
                finalProcessor);
//        ((SyncRequestProcessor)syncProcessor).start();        
		PrepRequestProcessor prepReqProcessor = new PrepRequestProcessor(zks, syncProcessor);
		RequestProcessorChain chain = new RequestProcessorChain(prepReqProcessor, 
				new StartableFromThread(syncProcessor), new StartableFromThread(prepReqProcessor));
		
		finalProcessor.setChain(chain);
		prepReqProcessor.setChain(chain);
		
		return chain;
	}
	
	public static RequestProcessorChainForFollower forFollower(FollowerZooKeeperServer zks){
		FinalRequestProcessor finalProcessor = new FinalRequestProcessor(zks);
		CommitProcessor commitProcessor = new CommitProcessor(finalProcessor,
		         Long.toString(zks.getServerId()), true);
//		commitProcessor.start();
		FollowerRequestProcessor firstProcessor = new FollowerRequestProcessor(zks, commitProcessor);
//		((FollowerRequestProcessor) firstProcessor).start();
		SyncRequestProcessor syncProcessor = new SyncRequestProcessor(zks,
		    		new SendAckRequestProcessor((Learner)zks.getFollower()));
//		syncProcessor.start();
		    
		RequestProcessorChainForFollower chain = new RequestProcessorChainForFollower(firstProcessor, 
				new StartableFromThread(commitProcessor), 
				new StartableFromThread(firstProcessor), 
				new StartableFromThread(syncProcessor));
		
		chain.setCommitProcessor(commitProcessor);
		chain.setSyncProcessor(syncProcessor);
		finalProcessor.setChain(chain);

		return chain;
	}

	public RequestProcessorChain start() {
		for(Startable thread : threads){
			thread.start();
		}
		return this;
	}
	
	public void processRequest(Request request) throws RequestProcessorException{
		firstProcessor.processRequest(request);
	}

	@Override
	public void shutdown() {
		
	}

	public static RequestProcessorChainForLeader forLeader(LeaderZooKeeperServer zks) {
		FinalRequestProcessor finalProcessor = new FinalRequestProcessor(zks);
        
        RequestProcessor toBeAppliedProcessor = new Leader.ToBeAppliedRequestProcessor(
                finalProcessor, zks.getLeader().toBeApplied);
        
        CommitProcessor commitProcessor = new CommitProcessor(toBeAppliedProcessor,
                Long.toString(zks.getServerId()), false);
//        commitProcessor.start();
        ProposalRequestProcessor proposalProcessor = new ProposalRequestProcessor(zks,
                commitProcessor);
        
//        proposalProcessor.initialize();
        PrepRequestProcessor firstProcessor = new PrepRequestProcessor(zks, proposalProcessor);
//        ((PrepRequestProcessor)firstProcessor).start();
	    
        RequestProcessorChainForLeader chain = new RequestProcessorChainForLeader(firstProcessor, 
        		new StartableFromThread(commitProcessor), 
				new StartableFromProposal(proposalProcessor), 
				new StartableFromThread(firstProcessor));
        
        chain.setCommitProcessor(commitProcessor);
        finalProcessor.setChain(chain);
        firstProcessor.setChain(chain);
        
        return chain;
	}
	
	static interface Startable {
		public void start();
	}
	
	private static class StartableFromThread implements Startable {
		private Thread t;
		
		public StartableFromThread(Thread t){
			this.t = t;
		}

		@Override
		public void start() {
			t.start();
		}
	}
	
	private static class StartableFromProposal implements Startable {
		private ProposalRequestProcessor t;

		public StartableFromProposal(ProposalRequestProcessor t){
			this.t = t;
		}

		@Override
		public void start() {
			t.initialize();
		}
	}
	
	public List<ChangeRecord> getOutstandingChanges() {
		return outstandingChanges;
	}

	public HashMap<String, ChangeRecord> getOutstandingChangesForPath() {
		return outstandingChangesForPath;
	}
}
