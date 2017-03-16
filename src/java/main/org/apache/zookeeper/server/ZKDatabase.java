package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.txn.TxnHeader;

public interface ZKDatabase {

	public static final int commitLogCount = 500;

	/**
	 * checks to see if the zk database has been
	 * initialized or not.
	 * @return true if zk database is initialized and false if not
	 */
	public abstract boolean isInitialized();

	/**
	 * clear the zkdatabase. 
	 * Note to developers - be careful to see that 
	 * the clear method does clear out all the
	 * data structures in zkdatabase.
	 */
	public abstract void clear();

	/**
	 * the datatree for this zkdatabase
	 * @return the datatree for this zkdatabase
	 */
	public abstract DataTree getDataTree();

	/**
	 * the committed log for this zk database
	 * @return the committed log for this zkdatabase
	 */
	public abstract long getmaxCommittedLog();

	/**
	 * the minimum committed transaction log
	 * available in memory
	 * @return the minimum committed transaction
	 * log available in memory
	 */
	public abstract long getminCommittedLog();

	/**
	 * Get the lock that controls the committedLog. If you want to get the pointer to the committedLog, you need
	 * to use this lock to acquire a read lock before calling getCommittedLog()
	 * @return the lock that controls the committed log
	 */
	public abstract ReentrantReadWriteLock getLogLock();

	public abstract LinkedList<Proposal> getCommittedLog();

	/**
	 * get the last processed zxid from a datatree
	 * @return the last processed zxid of a datatree
	 */
	public abstract long getDataTreeLastProcessedZxid();

	/**
	 * set the datatree initialized or not
	 * @param b set the datatree initialized to b
	 */
	public abstract void setDataTreeInit(boolean b);

	/**
	 * return the sessions in the datatree
	 * @return the data tree sessions
	 */
	public abstract Collection<Long> getSessions();

	/**
	 * get sessions with timeouts
	 * @return the hashmap of sessions with timeouts
	 */
	public abstract ConcurrentHashMap<Long, Integer> getSessionWithTimeOuts();

	/**
	 * load the database from the disk onto memory and also add 
	 * the transactions to the committedlog in memory.
	 * @return the last valid zxid on disk
	 * @throws IOException
	 */
	public abstract long loadDataBase() throws IOException;

	/**
	 * maintains a list of last <i>committedLog</i>
	 *  or so committed requests. This is used for
	 * fast follower synchronization.
	 * @param request committed request
	 */
	public abstract void addCommittedProposal(Request request);

	/**
	 * remove a cnxn from the datatree
	 * @param cnxn the cnxn to remove from the datatree
	 */
	public abstract void removeCnxn(ServerCnxn cnxn);

	/**
	 * kill a given session in the datatree
	 * @param sessionId the session id to be killed
	 * @param zxid the zxid of kill session transaction
	 */
	public abstract void killSession(long sessionId, long zxid);

	/**
	 * write a text dump of all the ephemerals in the datatree
	 * @param pwriter the output to write to
	 */
	public abstract void dumpEphemerals(PrintWriter pwriter);

	/**
	 * the node count of the datatree
	 * @return the node count of datatree
	 */
	public abstract int getNodeCount();

	/**
	 * the paths for  ephemeral session id 
	 * @param sessionId the session id for which paths match to 
	 * @return the paths for a session id
	 */
	public abstract HashSet<String> getEphemerals(long sessionId);

	/**
	 * the last processed zxid in the datatree
	 * @param zxid the last processed zxid in the datatree
	 */
	public abstract void setlastProcessedZxid(long zxid);

	/**
	 * the process txn on the data
	 * @param hdr the txnheader for the txn
	 * @param txn the transaction that needs to be processed
	 * @return the result of processing the transaction on this
	 * datatree/zkdatabase
	 */
	public abstract ProcessTxnResult processTxn(TxnHeader hdr, Record txn);

	/**
	 * stat the path 
	 * @param path the path for which stat is to be done
	 * @param serverCnxn the servercnxn attached to this request
	 * @return the stat of this node
	 * @throws KeeperException.NoNodeException
	 */
	public abstract Stat statNode(String path, ServerCnxn serverCnxn)
			throws KeeperException.NoNodeException;

	/**
	 * get the datanode for this path
	 * @param path the path to lookup
	 * @return the datanode for getting the path
	 */
	public abstract DataNode getNode(String path);

	/**
	 * convert from long to the acl entry
	 * @param aclL the long for which to get the acl
	 * @return the acl corresponding to this long entry
	 */
	public abstract List<ACL> convertLong(String path, Long aclL);

	/**
	 * get data and stat for a path 
	 * @param path the path being queried
	 * @param stat the stat for this path
	 * @param watcher the watcher function
	 * @return
	 * @throws KeeperException.NoNodeException
	 */
	public abstract byte[] getData(String path, Stat stat, Watcher watcher)
			throws KeeperException.NoNodeException;

	/**
	 * set watches on the datatree
	 * @param relativeZxid the relative zxid that client has seen
	 * @param dataWatches the data watches the client wants to reset
	 * @param existWatches the exists watches the client wants to reset
	 * @param childWatches the child watches the client wants to reset
	 * @param watcher the watcher function
	 */
	public abstract void setWatches(long relativeZxid,
			List<String> dataWatches, List<String> existWatches,
			List<String> childWatches, Watcher watcher);

	/**
	 * get acl for a path
	 * @param path the path to query for acl
	 * @param stat the stat for the node
	 * @return the acl list for this path
	 * @throws NoNodeException
	 */
	public abstract List<ACL> getACL(String path, Stat stat)
			throws NoNodeException;

	/**
	 * get children list for this path
	 * @param path the path of the node
	 * @param stat the stat of the node
	 * @param watcher the watcher function for this path
	 * @return the list of children for this path
	 * @throws KeeperException.NoNodeException
	 */
	public abstract List<String> getChildren(String path, Stat stat,
			Watcher watcher) throws KeeperException.NoNodeException;

	/**
	 * check if the path is special or not
	 * @param path the input path
	 * @return true if path is special and false if not
	 */
	public abstract boolean isSpecialPath(String path);

	/**
	 * get the acl size of the datatree
	 * @return the acl size of the datatree
	 */
	public abstract int getAclSize();

	/**
	 * Truncate the ZKDatabase to the specified zxid
	 * @param zxid the zxid to truncate zk database to
	 * @return true if the truncate is successful and false if not
	 * @throws IOException
	 */
	public abstract boolean truncateLog(long zxid) throws IOException;

	/**
	 * deserialize a snapshot from an input archive 
	 * @param ia the input archive you want to deserialize from
	 * @throws IOException
	 */
	public abstract void deserializeSnapshot(InputArchive ia)
			throws IOException;

	/**
	 * serialize the snapshot
	 * @param oa the output archive to which the snapshot needs to be serialized
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public abstract void serializeSnapshot(OutputArchive oa)
			throws IOException, InterruptedException;

	/**
	 * append to the underlying transaction log 
	 * @param si the request to append
	 * @return true if the append was succesfull and false if not
	 */
	public abstract boolean append(Request si) throws IOException;

	/**
	 * roll the underlying log
	 */
	public abstract void rollLog() throws IOException;

	/**
	 * commit to the underlying transaction log
	 * @param request 
	 * @throws IOException
	 */
	public abstract void commit(Request request) throws IOException;

	/**
	 * close this database. free the resources
	 * @throws IOException
	 */
	public abstract void close() throws IOException;
	
	public FileTxnSnapLog getSnapLog();

}