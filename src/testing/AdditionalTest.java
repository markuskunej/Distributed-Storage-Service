package testing;

import org.junit.Test;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;

public class AdditionalTest extends TestCase {

	KVServer kvServer1;
	KVServer kvServer2;
	KVServer kvServer3;
	KVServer kvServer4;
	KVServer kvServer5;
	
	KVStore kvClient1;
	KVStore kvClient2;
	KVStore kvClient3;
	KVStore kvClient4;
	KVStore kvClient5;

	public void setUp() {
		// Initially create 5 kvservers and 5 clients, connecting 1 to each server
		kvServer1 = new KVServer("localhost:7551", "localhost", 7552, 1, null);
		kvServer2 = new KVServer("localhost:7551", "localhost", 7553, 1, null);
		kvServer3 = new KVServer("localhost:7551", "localhost", 7554, 1, null);
		kvServer4 = new KVServer("localhost:7551", "localhost", 7555, 1, null);
		kvServer5 = new KVServer("localhost:7551", "localhost", 7556, 1, null);

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
	public void testComputeKeyRange() {

		assertTrue(true);
	}
}
