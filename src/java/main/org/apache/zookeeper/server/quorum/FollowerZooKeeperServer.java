/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.Record;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.RequestProcessorChain;
import org.apache.zookeeper.server.RequestProcessorChainForFollower;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: FollowerRequestProcessor -> CommitProcessor ->
 * FinalRequestProcessor
 * 
 * A SyncRequestProcessor is also spawned off to log proposals from the leader.
 */
public class FollowerZooKeeperServer extends LearnerZooKeeperServer {
    private static final Logger LOG =
        LoggerFactory.getLogger(FollowerZooKeeperServer.class);

    CommitProcessor commitProcessor;

    SyncRequestProcessor syncProcessor;

    /*
     * Pending sync requests
     */
    ConcurrentLinkedQueue<Request> pendingSyncs;
    
    /**
     * @param port
     * @param dataDir
     * @throws IOException
     */
    FollowerZooKeeperServer(FileTxnSnapLog logFactory,QuorumPeer self,
            DataTreeBuilder treeBuilder, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout,
                self.maxSessionTimeout, treeBuilder, zkDb, self);
        this.pendingSyncs = new ConcurrentLinkedQueue<Request>();
    }

    public Follower getFollower(){
        return self.follower;
    }      

    protected void setupRequestProcessorsOld() {
//        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
//        commitProcessor = new CommitProcessor(finalProcessor,
//                Long.toString(getServerId()), true);
//        commitProcessor.start();
//        firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
//        ((FollowerRequestProcessor) firstProcessor).start();
//        syncProcessor = new SyncRequestProcessor(this,
//                new SendAckRequestProcessor((Learner)getFollower()));
//        syncProcessor.start();
    }

    @Override
    protected void setupRequestProcessors() {
    	RequestProcessorChainForFollower chain = createChain();
    	commitProcessor = chain.getCommitProcessor();
    	syncProcessor = chain.getSyncProcessor();

    	defaultChain = chain.start();
    }

    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<Request>();

	private RequestProcessorChain defaultChain;
	
	@Override
	protected RequestProcessorChain getDefaultChain(){
		return defaultChain;
	}
	
	@Override
	protected RequestProcessorChainForFollower createChain() {
		return RequestProcessorChain.forFollower(this);
	}
	

    public void logRequest(TxnHeader hdr, Record txn) {
        Request request = new Request(null, hdr.getClientId(), hdr.getCxid(),
                hdr.getType(), null, null);
        request.hdr = hdr;
        request.txn = txn;
        request.zxid = hdr.getZxid();
        if ((request.zxid & 0xffffffffL) != 0) {
            pendingTxns.add(request);
        }
        
        ((RequestProcessorChainForFollower)getChain(request.getRootPath())).getSyncProcessor().processRequest(request);
//        syncProcessor.processRequest(request);
    }

    /**
     * When a COMMIT message is received, eventually this method is called, 
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    public void commit(long zxid) {
        if (pendingTxns.size() == 0) {
            LOG.warn("Committing " + Long.toHexString(zxid)
                    + " without seeing txn");
            return;
        }
        long firstElementZxid = pendingTxns.element().zxid;
        if (firstElementZxid != zxid) {
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)
                    + " but next pending txn 0x"
                    + Long.toHexString(firstElementZxid));
            System.exit(12);
        }
        Request request = pendingTxns.remove();
        
        ((RequestProcessorChainForFollower)getChain(request.getRootPath())).getCommitProcessor().commit(request);
//        commitProcessor.commit(request);
    }
    
    synchronized public void sync(){
        if(pendingSyncs.size() ==0){
            LOG.warn("Not expecting a sync.");
            return;
        }
                
        Request r = pendingSyncs.remove();
        
        ((RequestProcessorChainForFollower)getChain(r.getRootPath())).getCommitProcessor().commit(r);
        
//		commitProcessor.commit(r);
    }
             
    @Override
    public int getGlobalOutstandingLimit() {
        return super.getGlobalOutstandingLimit() / (self.getQuorumSize() - 1);
    }
    
    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        try {
            super.shutdown();
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }
        try {
            if (syncProcessor != null) {
                syncProcessor.shutdown();
            }
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception in syncprocessor shutdown",
                    e);
        }
    }
    
    @Override
    public String getState() {
        return "follower";
    }

    @Override
    public Learner getLearner() {
        return getFollower();
    }
}