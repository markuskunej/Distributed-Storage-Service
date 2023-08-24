package testing;

import app_kvServer.KVServer;
import client.KVStore;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;

import junit.framework.TestCase;

import static org.junit.Assert.assertTrue;

import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

import java.util.concurrent.TimeUnit;

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

	private Socket testSocket;
	private ServerSocket ECSServerSocket;

	String key = "TESTKEY";
	String val = "TESTVAL";
}
// 	public void setUp() {
// 		// Initially create 5 kvservers and 5 clients, connecting 1 to each server
// 		kvServer1 = new KVServer("localhost:7551", "localhost", 7552, 1, null, "test_data");
// 		kvServer2 = new KVServer("localhost:7551", "localhost", 7553, 1, null, "test_data");
// 		kvServer3 = new KVServer("localhost:7551", "localhost", 7554, 1, null, "test_data");
// 		kvServer4 = new KVServer("localhost:7551", "localhost", 7555, 1, null, "test_data");
// 		kvServer5 = new KVServer("localhost:7551", "localhost", 7556, 1, null, "test_data");

// 		kvClient1 = new KVStore("localhost", 7552);
// 		kvClient2 = new KVStore("localhost", 7553);
// 		kvClient3 = new KVStore("localhost", 7554);
// 		kvClient4 = new KVStore("localhost", 7555);
// 		kvClient5 = new KVStore("localhost", 7556);

// 		try {
// 			kvServer1.start();
// 			kvServer2.start();
// 			kvServer3.start();
// 			kvServer4.start();
// 			kvServer5.start();

// 			kvClient1.connect();
// 			kvClient2.connect();
// 			kvClient3.connect();
// 			kvClient4.connect();
// 			kvClient5.connect();

// 			kvServer1.putKV(key, val);
// 			kvServer2.putKV(key, val);
// 			kvServer3.putKV(key, val);

// 		} catch (Exception e) {
// 		}

// 	}

// 	PrivateKey privateKey;
// 	PublicKey publicKey;

// 	@Test
// 	public void keyGenTest() {
// 		// Test that public and private keys are generated
// 		// First by a storage server, then by a client
// 		KeyPairGenerator kpg = null;
// 		try {
// 			kpg = KeyPairGenerator.getInstance("RSA/ECB/PKCS1Padding");
// 		} catch (GeneralSecurityException e) {
// 			throw new RuntimeException(e);
// 		}	

// 		KeyPair keyPair = kpg.generateKeyPair();
// 		PrivateKey privateKey = keyPair.getPrivate();
// 		PublicKey publicKey = keyPair.getPublic();

// 		assertTrue(publicKey != null);
// 		assertTrue(privateKey != null);
// 		return;
// 	}

// 	@Test
// 	public void keyMatchTest() {
// 		// private and public keys must not be the same
// 		assertTrue(privateKey != publicKey);
// 	}

// 	byte[] temp;
// 	String test_phrase = "this is a sample test phrase";

// 	@Test
// 	public void encryptionTest() {
// 		// Create public and private keys
// 		keyGenTest();
// 		// convert test string to bytes
// 		byte[] test_bytes = test_phrase.getBytes();
// 		byte[] encrypted_test = kvClient1.encrypt(test_bytes, publicKey);
// 		// store encrypted array in temp - used for next test
// 		temp = encrypted_test;
// 		// compare both byte arrays - should be different
// 		assertTrue(test_bytes != encrypted_test);
// 	}

// 	@Test
// 	public void decryptionTest() {
// 		// Can use temp array as encrypted bytes
// 		byte[] decrypted_test = kvClient1.decrypt(temp, privateKey);
// 		// Convert bytes to string
// 		String test = new String(decrypted_test);
// 		// compare test and test_phrase, should be the same
// 		assertTrue(test == test_phrase);
// 	}

// 	@Test
// 	public void sendMessageTest() {
// 		// check if sendMessage runs properly
// 		Exception ex = null;

// 		byte[] test_bytes = test_phrase.getBytes();
		
// 		try {
// 			KVMessage msg = new KVMessage(test_bytes);
// 			kvClient1.sendMessage(msg);
// 		} catch (Exception e) {
// 			ex = e;
// 		}
// 		assertTrue(ex == null);
// 	}

// 	@Test
// 	public void receiveMessageTest() {
// 		// check if sendMessage runs properly
// 		Exception ex = null;
// 		Boolean enc = true;

// 		byte[] test_bytes = test_phrase.getBytes();
		
// 		try {
// 			KVMessage msg = new KVMessage(test_bytes);
// 			kvClient1.sendMessage(msg);
// 			kvClient1.receiveMessage(enc);
// 		} catch (Exception e) {
// 			ex = e;
// 		}
// 		assertTrue(ex == null);
// 	}

// 	@Test
// 	public void publicKeyExchangeTest() {
// 		// kvClient1 (for example) is a connected client, so kvServer1 should have kvClient1's public key
// 		PublicKey kvServerTest = kvServer1.getClientPublicKey();
// 		PublicKey kvClientTest = kvClient1.getClientPublicKey();

// 		assertTrue(kvServerTest == kvClientTest);
// 	}

// 	@Test
// 	public void serverInterceptTest() {
// 		Exception ex = null;
// 		// Public key intercepted, try decryption using that key (private keys are hidden)
// 		PublicKey kvServerIntercepted = kvServer1.getClientPublicKey();
// 		// Convert string to bytes
// 		byte[] test_bytes = test_phrase.getBytes();
// 		// Encrypt bytes
// 		byte[] encrypted = kvClient1.encrypt(test_bytes, kvServerIntercepted);
// 		// Try decrypting using a random private key
// 		PrivateKey wrong_key = kvClient5.getClientPrivateKey();

// 		try {
// 			byte[] decrypted = kvClient1.decrypt(encrypted, wrong_key);
// 		} catch(Exception e) {
// 			ex = e;
// 		}
// 		// The decryption will fail
// 		assertTrue(ex != null);
// 	}

// 	@Test
// 	public void BytesTest() {
// 		// For reference, this string is 260 characters --> 260 bytes
// 		String longString = "This is a testing string. This is a testing string. This is a testing string. This is a testing string. This is a testing string. This is a testing string. This is a testing string. This is a testing string. This is a testing string. This is a testing string. ";
// 		// Convert to bytes
// 		byte[] longBytes = longString.getBytes();
// 		// Run encryption and decryption
// 		byte[] encrypt = kvClient1.encrypt(longBytes, publicKey);
// 		byte[] decrypt = kvClient1.decrypt(encrypt, privateKey);

// 		assertTrue(encrypt.length > 245);
// 		assertTrue(decrypt.length > 245);
// 	}

// 	@Test
// 	public void reconnectTest() {
// 		kvClient1.disconnect();
// 		// Delay for 1 second
// 		TimeUnit.SECONDS.sleep(1);
// 		// Reconnect server
// 		kvClient1.connect();
// 		// Check serverPublicKey value
// 		PublicKey testKey = kvClient1.getServerPublicKey();
// 		// Get server's public key from server directly
// 		PublicKey serverKey = kvServer1.getServerPublicKey();
// 		assertTrue(testKey == serverKey);
// 	}
// }
