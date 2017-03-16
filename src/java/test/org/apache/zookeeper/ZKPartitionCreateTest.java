package org.apache.zookeeper;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;

public class ZKPartitionCreateTest extends ClientBase {

	private static class CreateRunnable implements Callable<Integer> {

		private ZooKeeper zk;
		private int i;
		private byte[] data;

		public CreateRunnable(ZooKeeper zk, int i) {
			this.zk = zk;
			this.i = i;
			this.data = "some".getBytes();
		}

		public Integer call() {
			try {
				zk.create("/" + i, data , Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				return 0;
			} catch (KeeperException e) {
				e.printStackTrace();
				return 0;
			} catch (InterruptedException e) {
				e.printStackTrace();
				return 0;
			}
		}
	}

	@Test
	public void testCreateMany() throws IOException, InterruptedException,
			KeeperException {
		System.out.println("Start test case:"+System.currentTimeMillis());
		ExecutorService pool = Executors.newFixedThreadPool(1000);
//		ExecutorService pool = Executors.newSingleThreadExecutor();
		
		List<Callable<Integer>> callables = new ArrayList<Callable<Integer>>();
		
		for (int i = 0; i < 500; i++) {
			callables.add(new CreateRunnable(createClient(),i));
		}
		
		long startTime = System.currentTimeMillis();
		System.out.println("Thread start time:"+ startTime);
		try{
			pool.invokeAll(callables);
		}catch(Throwable t){
			t.printStackTrace();
		}finally{
			pool.shutdown();
		}
		System.out.println("Thread time taken:" + (System.currentTimeMillis()-startTime));
	}
}
