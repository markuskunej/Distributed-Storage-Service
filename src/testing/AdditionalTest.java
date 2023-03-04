package testing;

import java.io.File;
import java.util.TreeMap;

import org.junit.Test;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

public class AdditionalTest extends TestCase {

	KVServer kvServer1;
	KVServer kvServer2;
	KVServer kvServer3;
	KVServer kvServer4;
	KVServer kvServer5;
	KVServer kvServer6;

	KVStore kvClient1;
	KVStore kvClient2;
	KVStore kvClient3;
	KVStore kvClient4;
	KVStore kvClient5;

	public void setUp() {
		// Initially create 5 kvservers and 5 clients, connecting 1 to each server
		kvServer1 = new KVServer("localhost:7551", "localhost", 7552, 1, null, "test_data");
		kvServer2 = new KVServer("localhost:7551", "localhost", 7553, 1, null, "test_data");
		kvServer3 = new KVServer("localhost:7551", "localhost", 7554, 1, null, "test_data");
		kvServer4 = new KVServer("localhost:7551", "localhost", 7555, 1, null, "test_data");
		kvServer5 = new KVServer("localhost:7551", "localhost", 7556, 1, null, "test_data");

		kvClient1 = new KVStore("localhost", 7552);
		kvClient2 = new KVStore("localhost", 7553);
		kvClient3 = new KVStore("localhost", 7554);
		kvClient4 = new KVStore("localhost", 7555);
		kvClient5 = new KVStore("localhost", 7556);

		try {
			kvServer1.start();
			kvServer2.start();
			kvServer3.start();
			kvServer4.start();
			kvServer5.start();

			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
			kvClient5.connect();

		} catch (Exception e) {
		}
	}

	// TODO add your test cases, at least 3
	@Test
	public void testFileCreation() {
		Exception ex = null;
		boolean exists = false;		
		try {
			// filename for kvServer1
			File f = new File("test_data/7552.properties");
			exists = f.exists();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && exists);
	}

	@Test
	public void testComputeKeyRange() {
		KVMessage response = null;
		Exception ex = null;
				
		try {
			//response = kvClient1.getKeyRanges();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.KEYRANGE_SUCCESS);
	}

	@Test
	public void testServerHash() {
		KVMessage response = null;
		Exception ex = null;
		String hash = "";
		// obtained by putting 127.0.0.1:7552 in md5 generator online	
		String expected_hash = "8b05321465768a5d2f3df532d00961d9";
		try {
			hash = kvServer1.getHash();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && hash.equals(expected_hash));
	}

	@Test
	public void testClientHash() {
		KVMessage response = null;
		Exception ex = null;
		String hash = "";
		// obtained by putting test in md5 generator online	
		String expected_hash = "098f6bcd4621d373cade4e832627b4f6";
		try {
			//hash = kvClient1.hash("test");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && hash.equals(expected_hash));
	}

	@Test
	public void testServerToServerConnection() {
		KVMessage response = null;
		Exception ex = null;
		
		try {
			//connect kvServer1 to kvServer2
			kvServer1.connectToServer("localhost", 7552);
			//close external server connection
			//kvServer1.closeServerConnection();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}

	@Test
	public void testMetadataUpdate() {
		TreeMap<String, String> orig_metadata = new TreeMap<String, String>();
		TreeMap<String, String> new_metadata = new TreeMap<String, String>();
		Exception ex = null;

		try {
			orig_metadata = kvServer2.getMetaData();
			// remove server 1 to trigger ECS to update metadata of all servers
			kvServer1.close();
			new_metadata = kvServer2.getMetaData();
		} catch (Exception e) {
			ex = e;
		}
		// assert no error, the new metadata is different from the old one, and kvServer 1 is no longer in the metadata
		assertTrue(ex == null && !orig_metadata.equals(new_metadata) && !new_metadata.containsKey(kvServer1.getName()));
	}

	@Test
	public void testAddKVServer() {
		Exception ex = null;

		try {
			kvServer6 = new KVServer("localhost:7551", "localhost", 7557, 1, null, "test_data");
			kvServer6.start();
		} catch (Exception e) {
			ex = e;
		}
		// assert no error, the new metadata is different from the old one, and kvServer 1 is no longer in the metadata
		assertTrue(ex == null);
	}

	@Test
	public void testFileDeleted() {
		boolean exists = true;
		Exception ex = null;

		try {
			kvServer2.close();
			// was server 2's filename
			File f = new File("test_data/7553.properties");
			exists = f.exists();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && !exists);
	}

	@Test
	public void testAddKVPairs() {
		Exception ex = null;
	
		try {
			kvClient1.put("k1", "v1");
			kvClient1.put("k2", "v2");
			kvClient2.put("k3", "v3");
			kvClient2.put("k4", "v4");
			kvClient3.put("k5", "v5");
			kvClient3.put("k6", "v6");
			kvClient4.put("k7", "v7");
			kvClient4.put("k8", "v8");
			kvClient5.put("k9", "v9");
			kvClient5.put("k10", "v10");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}

	@Test
	public void testRebalanceData() {
		Exception ex = null;
	
		try {
			// now that the servers contain data, try removing one
			kvServer3.close();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}
}
