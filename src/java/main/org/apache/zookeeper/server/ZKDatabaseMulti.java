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

package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class maintains the in memory database of zookeeper
 * server states that includes the sessions, datatree and the
 * committed logs. It is booted up  after reading the logs
 * and snapshots from the disk.
 */
public class ZKDatabaseMulti implements ZKDatabase {
    
    private static final Logger LOG = LoggerFactory.getLogger(ZKDatabaseMulti.class);
    
    private List<ZKDatabaseSingle> dbs = new ArrayList<ZKDatabaseSingle>();
    
    private Map<String, ZKDatabaseSingle> pathToDb = Collections.synchronizedMap(new HashMap<String, ZKDatabaseSingle>());

	private FileTxnSnapLog snapLog;
           
    
    /**
     * the filetxnsnaplog that this zk database
     * maps to. There is a one to one relationship
     * between a filetxnsnaplog and zkdatabase.
     * @param snapLog the FileTxnSnapLog mapping this zkdatabase
     */
    public ZKDatabaseMulti(FileTxnSnapLog snapLog) {
    	this.snapLog = snapLog;
//		dbs.add(new ZKDatabaseSingle(snapLog));
    }
    
    /**
     * checks to see if the zk database has been
     * initialized or not.
     * @return true if zk database is initialized and false if not
     */
    public boolean isInitialized() {
        return dbs.get(0).isInitialized();
    }
    
    /**
     * clear the zkdatabase. 
     * Note to developers - be careful to see that 
     * the clear method does clear out all the
     * data structures in zkdatabase.
     */
    public void clear() {
    	for (ZKDatabase db : dbs) {
			db.clear();
		}
    }
    
    /**
     * the datatree for this zkdatabase
     * @return the datatree for this zkdatabase
     */
    public DataTree getDataTree() {
        return dbs.get(0).dataTree;
    }
 
    /**
     * the committed log for this zk database
     * @return the committed log for this zkdatabase
     */
    public long getmaxCommittedLog() {
        return dbs.get(0).maxCommittedLog;
    }
    
    
    /**
     * the minimum committed transaction log
     * available in memory
     * @return the minimum committed transaction
     * log available in memory
     */
    public long getminCommittedLog() {
        return dbs.get(0).minCommittedLog;
    }
    /**
     * Get the lock that controls the committedLog. If you want to get the pointer to the committedLog, you need
     * to use this lock to acquire a read lock before calling getCommittedLog()
     * @return the lock that controls the committed log
     */
    public ReentrantReadWriteLock getLogLock() {
        return dbs.get(0).logLock;
    }
    

    public synchronized LinkedList<Proposal> getCommittedLog() {
    	return dbs.get(0).getCommittedLog();
    }      
    
    /**
     * get the last processed zxid from a datatree
     * @return the last processed zxid of a datatree
     */
    public long getDataTreeLastProcessedZxid() {
    	return 0;
//        return dbs.get(0).getDataTreeLastProcessedZxid();
    }
    
    /**
     * set the datatree initialized or not
     * @param b set the datatree initialized to b
     */
    public void setDataTreeInit(boolean b) {
        dbs.get(0).setDataTreeInit(b);
    }
    
    /**
     * return the sessions in the datatree
     * @return the data tree sessions
     */
    public Collection<Long> getSessions() {
    	return dbs.get(0).getSessions();
    }
    
    /**
     * get sessions with timeouts
     * @return the hashmap of sessions with timeouts
     */
    public ConcurrentHashMap<Long, Integer> getSessionWithTimeOuts() {
    	return new ConcurrentHashMap<Long, Integer>();
    	//TODO: ZKP: Just returning empty for now
//        return dbs.get(0).getSessionWithTimeOuts();
    }

    
    /**
     * load the database from the disk onto memory and also add 
     * the transactions to the committedlog in memory.
     * @return the last valid zxid on disk
     * @throws IOException
     */
    public long loadDataBase() throws IOException {
    	return dbs.get(0).loadDataBase();
    }
    
    /**
     * maintains a list of last <i>committedLog</i>
     *  or so committed requests. This is used for
     * fast follower synchronization.
     * @param request committed request
     */
    public void addCommittedProposal(Request request) {
    	//TODO: ZKP: Yet to figure out
//    	dbs.get(0).addCommittedProposal(request);
    }

    
    /**
     * remove a cnxn from the datatree
     * @param cnxn the cnxn to remove from the datatree
     */
    public void removeCnxn(ServerCnxn cnxn) {
//        dbs.get(0).removeCnxn(cnxn);
    }

    /**
     * kill a given session in the datatree
     * @param sessionId the session id to be killed
     * @param zxid the zxid of kill session transaction
     */
    public void killSession(long sessionId, long zxid) {
        dbs.get(0).killSession(sessionId, zxid);
    }

    /**
     * write a text dump of all the ephemerals in the datatree
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        dbs.get(0).dumpEphemerals(pwriter);
    }

    /**
     * the node count of the datatree
     * @return the node count of datatree
     */
    public int getNodeCount() {
    	return dbs.get(0).getNodeCount();
    }

    /**
     * the paths for  ephemeral session id 
     * @param sessionId the session id for which paths match to 
     * @return the paths for a session id
     */
    public HashSet<String> getEphemerals(long sessionId) {
    	//TODO: ZKP: Currently returning empty set
    	return new HashSet<String>();
//        return dbs.get(0).getEphemerals(sessionId);
    }

    /**
     * the last processed zxid in the datatree
     * @param zxid the last processed zxid in the datatree
     */
    public void setlastProcessedZxid(long zxid) {
        dbs.get(0).setlastProcessedZxid(zxid);
    }

    /**
     * the process txn on the data
     * @param hdr the txnheader for the txn
     * @param txn the transaction that needs to be processed
     * @return the result of processing the transaction on this
     * datatree/zkdatabase
     */
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
        switch (hdr.getType()) {
        case OpCode.create:
            CreateTxn createTxn = (CreateTxn) txn;
            String path = createTxn.getPath();
            ZKDatabaseSingle db = resolveDb(path);
            return db.processTxn(hdr, createTxn);
            
        //create/close session don't require request record
        case OpCode.createSession:
        case OpCode.closeSession:
        	//TODO: ZKP: Just returning dummy for now
        	return new ProcessTxnResult();
//        	return dbs.get(0).processTxn(hdr, txn);
        }
        LOG.info("processTxn:Op:"+hdr.getType());
        return dbs.get(0).processTxn(hdr, txn);
    }

	private ZKDatabaseSingle resolveDb(String path) {
		//TODO: ZKP: This is invoked via parentPath. What to do in that case
		if(path.isEmpty()){
			return dbs.get(0);
		}
		
//		if(pathToDb.size() > 0){
//			for(Map.Entry<String, ZKDatabaseSingle> dbEntry : pathToDb.entrySet()){
//				return dbEntry.getValue();
//			}
//		}

		String rhs = path.split("/", 2)[1];
		String root = rhs.split("/", 2)[0];
		LOG.info("ZKP: Root path:"+root);
		
		if(!pathToDb.containsKey(root)){
			LOG.info("ZKP: Creating new data tree for path: " + root);
			try {
				pathToDb.put(root, new ZKDatabaseSingle(new FileTxnSnapLog(snapLog.getDataDir(), snapLog.getSnapDir())));
			} catch (IOException e) {
				LOG.error("Error initializing data and/or snapshot dir", e);
			}
		}
		return pathToDb.get(root);
	}

    /**
     * stat the path 
     * @param path the path for which stat is to be done
     * @param serverCnxn the servercnxn attached to this request
     * @return the stat of this node
     * @throws KeeperException.NoNodeException
     */
    public Stat statNode(String path, ServerCnxn serverCnxn) throws KeeperException.NoNodeException {
        return resolveDb(path).statNode(path, serverCnxn);
    }
    
    /**
     * get the datanode for this path
     * @param path the path to lookup
     * @return the datanode for getting the path
     */
    public DataNode getNode(String path) {
    	return resolveDb(path).getNode(path);
//      return dbs.get(0).getNode(path);
    }

    /**
     * convert from long to the acl entry
     * @param aclL the long for which to get the acl
     * @return the acl corresponding to this long entry
     */
    public List<ACL> convertLong(String path, Long aclL) {
        return resolveDb(path).convertLong(path, aclL);
    }

    /**
     * get data and stat for a path 
     * @param path the path being queried
     * @param stat the stat for this path
     * @param watcher the watcher function
     * @return
     * @throws KeeperException.NoNodeException
     */
    public byte[] getData(String path, Stat stat, Watcher watcher) 
    throws KeeperException.NoNodeException {
        return resolveDb(path).getData(path, stat, watcher);
    }

    /**
     * set watches on the datatree
     * @param relativeZxid the relative zxid that client has seen
     * @param dataWatches the data watches the client wants to reset
     * @param existWatches the exists watches the client wants to reset
     * @param childWatches the child watches the client wants to reset
     * @param watcher the watcher function
     */
    public void setWatches(long relativeZxid, List<String> dataWatches,
            List<String> existWatches, List<String> childWatches, Watcher watcher) {
        dbs.get(0).setWatches(relativeZxid, dataWatches, existWatches, childWatches, watcher);
    }
    
    /**
     * get acl for a path
     * @param path the path to query for acl
     * @param stat the stat for the node
     * @return the acl list for this path
     * @throws NoNodeException
     */
    public List<ACL> getACL(String path, Stat stat) throws NoNodeException {
        return resolveDb(path).getACL(path, stat);
    }

    /**
     * get children list for this path
     * @param path the path of the node
     * @param stat the stat of the node
     * @param watcher the watcher function for this path
     * @return the list of children for this path
     * @throws KeeperException.NoNodeException
     */
    public List<String> getChildren(String path, Stat stat, Watcher watcher)
    throws KeeperException.NoNodeException {
        return resolveDb(path).getChildren(path, stat, watcher);
    }

    /**
     * check if the path is special or not
     * @param path the input path
     * @return true if path is special and false if not
     */
    public boolean isSpecialPath(String path) {
        return resolveDb(path).isSpecialPath(path);
    }

    /**
     * get the acl size of the datatree
     * @return the acl size of the datatree
     */
    public int getAclSize() {
        return dbs.get(0).getAclSize();
    }

    /**
     * Truncate the ZKDatabase to the specified zxid
     * @param zxid the zxid to truncate zk database to
     * @return true if the truncate is successful and false if not
     * @throws IOException
     */
    public boolean truncateLog(long zxid) throws IOException {
        return dbs.get(0).truncateLog(zxid);
    }
    
    /**
     * deserialize a snapshot from an input archive 
     * @param ia the input archive you want to deserialize from
     * @throws IOException
     */
    public void deserializeSnapshot(InputArchive ia) throws IOException {
    	dbs.get(0).deserializeSnapshot(ia);
    }   
    
    /**
     * serialize the snapshot
     * @param oa the output archive to which the snapshot needs to be serialized
     * @throws IOException
     * @throws InterruptedException
     */
    public void serializeSnapshot(OutputArchive oa) throws IOException,
    InterruptedException {
    	dbs.get(0).serializeSnapshot(oa);
    }

    /**
     * append to the underlying transaction log 
     * @param si the request to append
     * @return true if the append was succesfull and false if not
     */
    public boolean append(Request request) throws IOException {
//    	return false;
        switch (request.type) {
	        case OpCode.create:              
//	        	return true;
	        	CreateRequest createRequest = new CreateRequest();
	        	request.request.rewind();
	            ByteBufferInputStream.byteBuffer2Record(request.request, (Record)createRequest);
	            String path = createRequest.getPath();
	            return resolveDb(path).append(request);
        }
        return false;
    }

    /**
     * roll the underlying log
     */
    public void rollLog() throws IOException {
    	dbs.get(0).rollLog();
    }

    /**
     * commit to the underlying transaction log
     * @throws IOException
     */
    public void commit(Request request) throws IOException {
        switch (request.type) {
        case OpCode.create:              
        	CreateRequest createRequest = new CreateRequest();
        	request.request.rewind();
            ByteBufferInputStream.byteBuffer2Record(request.request, (Record)createRequest);
            String path = createRequest.getPath();
            resolveDb(path).commit(request);
        }
    }
    
    /**
     * close this database. free the resources
     * @throws IOException
     */
    public void close() throws IOException {
//        dbs.get(0).close();
    }

	@Override
	public FileTxnSnapLog getSnapLog() {
		return dbs.get(0).getSnapLog();
	}
    
}
