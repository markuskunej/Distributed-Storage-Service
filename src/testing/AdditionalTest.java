package testing;

import java.beans.Transient;
import java.io.File;
import java.util.TreeMap;

import app_kvServer.KVServer;
import client.KVStore;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

import app_kvECS.ECSClient;

import shared.messages.IECSMessage;
import shared.messages.ECSMessage;

import java.net.ServerSocket;
import java.net.Socket;

import ecs.KVServerConnection;

import java.util.TreeSet;

import javax.accessibility.AccessibleHypertext;

import java.util.Map;
import java.util.TreeMap;

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

	private Socket testSocket;
	private ServerSocket ECSServerSocket;

	String key = "TESTKEY";
	String val = "TESTVAL";

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

			kvServer1.putKV(key, val);
			kvServer2.putKV(key, val);
			kvServer3.putKV(key, val);

		} catch (Exception e) {
		}

	}

	// TODO add your test cases, at least 3
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
	public void testRemoveKVServer() {
		Exception ex = null;

		try {
			kvServer4.close();
		} catch(Exception e) {
			ex = e;
		}
		// assert no error, metadata updates, etc.
		assertTrue(ex == null);
	}

	@Test
	public void testFailureDetection() {
		Exception ex = null;
		// KVServerConnection.emptyReceived is a private field, so we can just assume it is 2 for this test
		
		// Since kvserver4 was previously closed, we can treat it as our "failed" server
		try {
			ECSClient client;
			testSocket = ECSServerSocket.accept();
            
            String tempName = Integer.toString(testSocket.getPort());
            KVServerConnection connection = new KVServerConnection(testSocket, client, tempName);

			new Thread(connection).start();

		} catch (IOException e) {
			ex = e;
		}
		// The server connection should have failed
		assertTrue(ex != null);
	}

	@Test
	public void testRecovery() {
		// With server 4 closed, 5 and 6 should still have identical values
		Exception ex = null;
		Treemap meta5;
		Treemap meta6;
		
		try {
			meta5 = kvServer5.getMetaData();
			meta6 = kvServer6.getMetaData();
		} catch (Exception e) {
			ex = e;
		}

		// no error occured and metadata are equal
		assertTrue(ex == null);
		assertTrue(meta5 == meta6);
	}

	@Test
	public void testKeyRangeRead() {
		// servers 1-3 constitute a coordinator and two replicas, so the complete keyRangeRead
		// should go from the start of 1 to the end of 3
		Exception ex;

		String complete;
		String test;

		try {
			String range1 = kvServer1.getKeyrange();
			String range3 = kvServer3.getKeyrange();

			String[] server1 = range1.split(",");
			String start = server1[0];

			String[] server3 = range3.split(",");
			String end = server3[1];

			complete = start + "," + end;

			String keyrangeread = kvServer1.getKeyrangeRead();
			String[] temp = keyrangeread.split(",");
			test = temp[0] + "," + temp[1];
		} catch (Exception e) {
			ex = e;
		}

		// no errors occured and key range is complete
		assertTrue(ex == null);
		assertTrue(complete == test);
	}

	@Test
	public void testCoordToReplicaConnect() {
		// Servers 5 and 6 are a coordinator replica pair, so we test their connection

		KVMessage response = null;
		Exception ex = null;
		
		try {
			kvServer5.connectToServer("localhost", 7557);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}

	@Test
	public void testReplica() {
		// Testing if server x and y have the same KV pair - currently testing 1 and 2
		Exception ex = null;
		Treemap meta1;
		Treemap meta2;
		
		String val1;
		String val2;

		try {
			meta1 = kvServer1.getMetaData();
			meta2 = kvServer2.getMetaData();

			val1 = kvServer1.getKV(key);
			val2 = kvServer2.getKV(key);
		} catch (Exception e) {
			ex = e;
		}

		// no error occured and metadata and KV are equal
		assertTrue(ex == null);
		assertTrue(meta1 == meta2);
		assertTrue(val1 == val2);
	}
}
