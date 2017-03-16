package org.apache.zookeeper.server;

public class ZKPartitionUtils {
	
	public static String getRootPath(String path) {
		String rhs = path.split("/", 2)[1];
		String root = rhs.split("/", 2)[0];
		return root;
	}

}
