package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.cli.*;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;
import server.ECSMessageHandler;
import server.KVClientConnection;
import server.ServerMessageHandler;
import shared.messages.ECSMessage;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.messages.IECSMessage;

import java.util.Base64;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import shared.messages.IECSMessage;

import java.security.GeneralSecurityException;
// Encryption Imports
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.X509EncodedKeySpec;
import javax.crypto.Cipher;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private String ecs_addr;
	private int ecs_port;
	private String address;
	private int port;
	private ServerSocket serverSocket;
	private Socket successorServerSocket;
	private Socket ecsSocket;
	private boolean running;
	private int cacheSize;
	private CacheStrategy strategy;
	private String fileName;
	private boolean stopped;
	private boolean write_lock;
	private OutputStream ecs_output;
	private InputStream ecs_input;
	private String dataDir;
	private TreeMap<String, String> metadata;
	private String serverHash;
	private ECSMessageHandler ecsHandler;
	private ServerMessageHandler serverMsgHandler;

	private ArrayList keySet;
	private int keyCounter;
	private Properties cache;

	private ArrayList lfuFreq;

	private static PrivateKey serverPrivateKey;
	private static PublicKey serverPublicKey;
	private PublicKey clientPublicKey;
	private PublicKey otherServerPublicKey;

	static {
		// Generate public/private key
		KeyPairGenerator kpg = null;
		try {
			kpg = KeyPairGenerator.getInstance("RSA/ECB/PKCS1Padding");
		} catch (GeneralSecurityException e) {
			throw new RuntimeException(e);
		}	

		KeyPair serverKeyPair = kpg.generateKeyPair();
		serverPrivateKey = serverKeyPair.getPrivate();
		serverPublicKey = serverKeyPair.getPublic();
	}

	/**
	 * Start KV Server at given port
	 * 
	 * @param port      given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *                  to keep in-memory
	 * @param strategy  specifies the cache replacement strategy in case the cache
	 *                  is full and there is a GET- or PUT-request on a key that is
	 *                  currently not contained in the cache. Options are "FIFO",
	 *                  "LRU",
	 *                  and "LFU".
	 */
	public KVServer(String ecs_ip_port, String address, int port, int cacheSize, CacheStrategy strategy, String dataDir) {
		String[] ecs = ecs_ip_port.split(":");
		this.ecs_addr = ecs[0];
		this.ecs_port = Integer.parseInt(ecs[1]);
		this.address = address;
		this.port = port;
		this.serverHash = hash(address + ":" + port);
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.dataDir = dataDir;
		this.stopped = true; // start in stopped state
		this.write_lock = false;
		this.metadata = new TreeMap<String, String>();
		
		this.keySet = new ArrayList<String>();
		this.keyCounter = 0;
		this.cache = new Properties();

		this.lfuFreq = new ArrayList<Integer>();
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public String getHostname() {
		// try {
		// InetAddress sv = InetAddress.getLocalHost();

		// return sv.getHostName();
		// } catch (UnknownHostException ex) {
		// logger.error("Error! Unknown Host. \n", ex);

		// return null;
		// }
		return null;
	}

	public String getNameServer() {
		return address + ":" + port;
	}

	public String getHash() {
		return serverHash;
	}

	public PublicKey getPublicKey() {
		return serverPublicKey;
	}

	public PrivateKey getPrivateKey() {
		return serverPrivateKey;
	}

	private PublicKey strToPublicKey (String key) {
		try {
			X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(key.getBytes());
			KeyFactory kf = KeyFactory.getInstance("RSA/ECB/PKCS1Padding");

			return kf.generatePublic(X509publicKey);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public void setClientPublicKey(String key){
			//byte[] byteKey = Base64.getDecoder()(key.getBytes());
		this.clientPublicKey = strToPublicKey(key);
	}

	public void setOtherServerPublicKey(String key){
		try{
			//byte[] byteKey = Base64.getDecoder()(key.getBytes());
			this.otherServerPublicKey = strToPublicKey(key);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return this.strategy;
	}

	@Override
	public int getCacheSize() {
		return this.cacheSize;
	}

	public TreeMap<String, String> getMetaData() {
        return metadata;
    }

	public String md5SubtractOne(String hash) {
		BigInteger decimalBigInt = new BigInteger(hash, 16); // Parse hex string as a BigInteger
		decimalBigInt = decimalBigInt.subtract(BigInteger.ONE); // Subtract 1 from the BigInteger
		String resultHexString = decimalBigInt.toString(16); // Convert the result back to a hex string
		while (resultHexString.length() < 32) {
			resultHexString = "0" + resultHexString; // Pad result with leading zeros if necessary
		}
		return resultHexString;
	}

	public String md5AddOne(String hash) {
		BigInteger decimalBigInt = new BigInteger(hash, 16); // Parse hex string as a BigInteger
		decimalBigInt = decimalBigInt.add(BigInteger.ONE); // add 1 from the BigInteger
		String resultHexString = decimalBigInt.toString(16); // Convert the result back to a hex string
		while (resultHexString.length() < 32) {
			resultHexString = "0" + resultHexString; // Pad result with leading zeros if necessary
		}
		return resultHexString;
	}

	public String getKeyrange() {
		StringBuilder keyrange_str = new StringBuilder();
		for (String serverHash : metadata.keySet()) {
			String startHash = metadata.lowerKey(serverHash);
			if (startHash == null) {
				// at the last server in hash ring, make end hash one less than first hash
				startHash = metadata.lastKey();
			}
			String serverName = metadata.get(serverHash);
			keyrange_str.append(md5AddOne(startHash) + "," + serverHash + "," + serverName + ";");
		}
		keyrange_str.append("\r\n");

		return keyrange_str.toString();
	}

	public String getKeyrangeRead() {
		StringBuilder keyrange_str = new StringBuilder();
		if (metadata.size() <= 3) {
			// less than or 3 servers, read range is entire ring starting at their hash
			for (String serverHash : metadata.keySet()) {
				String serverName = metadata.get(serverHash);
				keyrange_str.append(md5AddOne(serverHash) + "," + serverHash + "," + serverName + ";");
			}
		} else {
			for (String serverHash : metadata.keySet()) {
				String currentHash = serverHash;
				//get the 3rd predecessor
				for (int i = 0; i < 3; i++) {
					currentHash = metadata.lowerKey(currentHash);
					if (currentHash == null) {
						// at the last server in hash ring, make end hash one less than first hash
						currentHash = metadata.lastKey();
					}
				}
				String serverName = metadata.get(serverHash);
				keyrange_str.append(md5AddOne(currentHash) + "," + serverHash + "," + serverName + ";");
			}
		}
		keyrange_str.append("\r\n");

		return keyrange_str.toString();
	}

	public void setMetaData(TreeMap<String, String> metadata) {
		this.metadata = metadata;
	}

	public void setStopped(boolean stopped) {
		this.stopped = stopped;
	}

	public void setWriteLock(boolean lock) {
		this.write_lock = lock;
	}

	@Override
	public boolean inStorage(String key) {
		// try {
		// // boolean exists = (storage.getKV(key) != null);
		// boolean exists = false;

		// if (exists) {
		// logger.info("Key :: " + key + " found in storage. \n");
		// } else {
		// logger.info("Key :: " + key + " not found in storage. \n");
		// }

		// return exists;

		// } catch (Exception e) {
		// logger.error("IO Failure. \n", e);

		// return false;
		// }
		return false;
	}

	private String hash(String input_str) {
        return DigestUtils.md5Hex(input_str);
    }

	@Override
	public boolean inCache(String key) {
		if (this.strategy == CacheStrategy.None) {
			return false;
		}

		// boolean exists = (cache.getKV(key) != null);
		boolean exists = false;
		if (exists) {
			logger.info("Key :: " + key + " found in cache. \n");
		} else {
			logger.info("Key :: " + key + " not found in cache. \n");
		}

		return exists;
	}

	public boolean isResponsible (String key, boolean isWrite) {
		if (isWrite) {
			return isWriteResponsible(key);
		} else {
			return isReadResponsible(key);
		}
	}

	public boolean isWriteResponsible (String key) {
		try {
			if (metadata != null) {
				String hash = DigestUtils.md5Hex(key);
				Map.Entry<String, String> coordinator_entry = metadata.ceilingEntry(hash);
				
				if (coordinator_entry == null) {
					//this means the key's hash is lower then all the servers, the responsible server would then be the largest hash
					coordinator_entry = metadata.firstEntry();
				}
				// see if hashes match
				return (serverHash.equals(coordinator_entry.getKey()));

			} else {
				logger.error("Server doesn't have metadata");
				System.exit(1);
				return false;
			}
		} catch (Exception e) {
			logger.error("Error in isWriteResponsible");
			System.exit(1);
			return false;
		}
	}

	public boolean isReadResponsible (String key) {
		try {
			if (metadata != null) {
				String hash = DigestUtils.md5Hex(key);
				Map.Entry<String, String> coordinator_entry = metadata.ceilingEntry(hash);
				if (coordinator_entry == null) {
					//this means the key's hash is lower then all the servers, the responsible server would then be the largest hash
					coordinator_entry = metadata.firstEntry();
				}
				if (serverHash.equals(coordinator_entry.getKey())) {
					return true;
				}

				if (metadata.size() > 1) {
					Map.Entry<String, String> replica_1_entry = getNextServer(coordinator_entry.getKey());
					if (replica_1_entry != null) {
						if (serverHash.equals(replica_1_entry.getKey())) {
							return true;
						}
						if (metadata.size() > 2) {
							Map.Entry<String, String> replica_2_entry = getNextServer(replica_1_entry.getKey());
							if (replica_2_entry != null && serverHash.equals(replica_2_entry.getKey())) {
								return true;
							}
						}
					}
				}

				return false;

			} else {
				logger.error("Server doesn't have metadata");
				System.exit(1);
				return false;
			}
		} catch (Exception e) {
			logger.error("Error in isReadResponsible");
			System.exit(1);
			return false;
		}
	}

	private Map.Entry<String, String> getNextServer(String server_key) {
		if (metadata != null && metadata.size() > 1) {
			Map.Entry<String, String> next_server = metadata.higherEntry(server_key);
			if (next_server == null) {
				next_server = metadata.firstEntry();
			}
			return next_server;
		} else {
			// metadata null or only 1 server in it, so there is no "next server"
			return null;
		}
	}

	@Override
	public String getKV(String key) throws Exception {
		// Try cache first
		String val = cache.getProperty(key);

		if (val != null) {
			logger.info("Value found in cache");
			// update LRU/LFU list, FIFO does not change
			int index;
			switch (this.strategy) {
				case LRU:
				// Move key from previous position to the front - it is most recently used
				index = this.keySet.indexOf(key);
				this.keySet.remove(index);
				this.keySet.add(0, key);
				break;
				case FIFO:
				logger.info("FIFO replacement strategy: no modification required");
				break;
				case LFU:
				// LFU
				index = this.keySet.indexOf(key);
				Integer current = (Integer)this.lfuFreq.get(index);
				this.lfuFreq.set(index, (current + 1));
				break;
				default:
				logger.error("Replacement Strategy error: Please ensure proper replacement strategy value");
			}


			return val;
		}

		// If not in cache, check deep storage
		

		try (InputStream input = new FileInputStream(fileName)) {

			Properties prop = new Properties();

			// load the kv storage
			prop.load(input);

			// get value
			String value = prop.getProperty(key);

			if (value != null) {
				logger.info("Key :: " + key + ", Value :: " + value + "\n");
			} else {
				// value doesn't exist
				logger.info("Key :: " + key + " has no associated value. \n");
			}

			input.close();

			// Check if cache is full
			if (this.keyCounter == this.cacheSize) {
				int index;
				String removeKey;
				switch (this.strategy) {
					case LRU:
					// LRU
					// remove last (least recently used) element of array
					index = this.keyCounter - 1;
					removeKey = (String)this.keySet.get(index);
					this.keySet.remove(index);
					this.cache.remove(removeKey);
					// insert key/value taken from deep storage into the cache
					this.keySet.add(0, key);
					this.cache.setProperty(key, value);
					break;
					case FIFO:
					// FIFO
					// remove last element of array
					index = this.keyCounter - 1;
					removeKey = (String)this.keySet.get(index);
					this.keySet.remove(index);
					this.cache.remove(removeKey);
					// insert key/value taken from deep storage into the cache
					this.keySet.add(0, key);
					this.cache.setProperty(key, value);
					break;
					case LFU:
					// LFU
					// Find minimum value in list
					Integer min = (Integer)Collections.min(this.lfuFreq);
					// If duplicate minimums exist, remove first occurence (arbitrarily)
					index = this.lfuFreq.indexOf(min);
					removeKey = (String)this.keySet.get(index);
					this.keySet.remove(index);
					this.lfuFreq.remove(index);
					this.cache.remove(removeKey);
					// insert new value with frequency of 1
					this.keySet.add(0, key);
					this.lfuFreq.add(0, 1);
					this.cache.setProperty(key, value);
					break;
					case None:
					// No cache
					break;
					default:
					logger.error("Replacement Strategy error: Please ensure proper replacement strategy value");
				}
			} else {
				// not full, just write to cache
				int index;
				switch (this.strategy) {
					case LRU:
					// LRU
					// insert key/value taken from deep storage into the cache
					this.keySet.add(0, key);
					this.cache.setProperty(key, value);
					break;
					case FIFO:
					// FIFO
					// insert key/value taken from deep storage into the cache
					this.keySet.add(0, key);
					this.cache.setProperty(key, value);
					break;
					case LFU:
					// LFU
					// insert new value with frequency of 1
					this.keySet.add(0, key);
					this.lfuFreq.add(0, 1);
					this.cache.setProperty(key, value);
					break;
					case None:
					// No cache
					break;
					default:
					logger.error("Replacement Strategy error: Please ensure proper replacement strategy value");
				}
			}

			return value;

		} catch (Exception e) {
			logger.error("ERROR in KVServer.getKV \n", e);

			return null;
		}
	}

	@Override
	public synchronized StatusType putKV(String key, String value) throws Exception {
		// check if value is null - delete operation

		// NEW - for encryption, strings cannot be empty ("")
		// "" has been replaced with "EMPTY STRING"

		if (value == null || value == "" || value == "EMPTY STRING") {
			// check if value is in cache
			String val = cache.getProperty(key);
			if (val != null) {
				// There exists a key-value pair in the cache, remove depends on cache strategy
				// By default, start status as DELETE_ERROR;
				StatusType status = StatusType.DELETE_ERROR;
				int index;
				switch (this.strategy) {
					case LRU:
					index = this.keySet.indexOf(key);
					this.keySet.remove(index);
					this.keyCounter -= 1;
					this.cache.remove(key);

					status = StatusType.DELETE_SUCCESS;
					logger.info("Deleting key-value pair from cache: Key :: " + key + ", Value :: " + val.toString() + "\n");
					break;
					case FIFO:
					index = this.keySet.indexOf(key);
					this.keySet.remove(index);
					this.keyCounter -= 1;
					this.cache.remove(key);

					status = StatusType.DELETE_SUCCESS;
					logger.info("Deleting key-value pair from cache: Key :: " + key + ", Value :: " + val.toString() + "\n");
					break;
					case LFU:
					index = this.keySet.indexOf(key);
					this.keySet.remove(index);
					this.lfuFreq.remove(index);
					this.keyCounter -= 1;
					this.cache.remove(key);

					status = StatusType.DELETE_SUCCESS;
					logger.info("Deleting key-value pair from cache: Key :: " + key + ", Value :: " + val.toString() + "\n");
					break;
					case None:
					// no cache
					break;
					default:
					logger.error("Replacement Strategy error: Please ensure proper replacement strategy value");
				}
			}
		} else {
			// Write to the cache first
			if (this.keyCounter == this.cacheSize) {
				String val;
				String removeKey;
				switch (this.strategy) {
					case LRU:
					// LRU
					// Check if it's in the cache already
					val = cache.getProperty(key);
					// if in the cache, shift it to the front
					if (val != null) {
						int index = this.keySet.indexOf(key);
						this.keySet.remove(index);
						// re-insert the key/value into the cache
						this.keySet.add(0, key);
						this.cache.setProperty(key, value);
					} else {
						int index = this.keyCounter - 1;
						removeKey = (String)this.keySet.get(index);
						this.keySet.remove(index);
						this.cache.remove(removeKey);
						// insert the key/value into the cache
						this.keySet.add(0, key);
						this.cache.setProperty(key, value);
					}
					break;
					case FIFO:
					// FIFO
					// Check if it's in the cache already
					val = cache.getProperty(key);
					// if in the cache, leave it be
					if (val == null) {
						// if not in the cache, remove the last element and insert the new element
						int index = this.keyCounter - 1;
						removeKey = (String)this.keySet.get(index);
						this.keySet.remove(index);
						this.cache.remove(removeKey);
						// insert the key/value into the cache
						this.keySet.add(0, key);
						this.cache.setProperty(key, value);
					}
					// if not in the cache, insert in the front
					break;
					case LFU:
					// LFU
					// Check if it's in the cache already
					val = cache.getProperty(key);
					// if in the cache, just update frequency value
					if (val != null) {
						int index = this.keySet.indexOf(key);
						Integer current = (Integer)this.lfuFreq.get(index);
						this.lfuFreq.set(index, (current + 1));
					// if not in the cache, remove lowest frequency value and insert new value with frequency of 1
					} else {
						Integer min = (Integer)Collections.min(this.lfuFreq);
						// If duplicate minimums exist, remove first occurence (arbitrarily)
						int index = this.lfuFreq.indexOf(min);
						removeKey = (String)this.keySet.get(index);
						this.keySet.remove(index);
						this.lfuFreq.remove(index);
						this.cache.remove(removeKey);
						// insert new value with frequency of 1
						this.keySet.add(0, key);
						this.lfuFreq.add(0, 1);
						this.cache.setProperty(key, value);
					}
					break;
					case None:
					// None
					break;
					default:
					logger.error("Replacement Strategy error: Please ensure proper replacement strategy value");
				}
			} else {
				// cache is not full
				String val;
				switch (this.strategy) {
					case LRU:
					// LRU
					// Check if it's in the cache already
					val = cache.getProperty(key);
					// if in the cache, shift it to the front
					if (val != null) {
						int index = this.keySet.indexOf(key);
						this.keySet.remove(index);
						// re-insert the key/value into the cache
						this.keySet.add(0, key);
						this.cache.setProperty(key, value);
					} else {
						// insert the key/value into the cache
						this.keySet.add(0, key);
						this.cache.setProperty(key, value);
						this.keyCounter += 1;
					}
					break;
					case FIFO:
					// FIFO
					// Check if it's in the cache already
					val = cache.getProperty(key);
					// if in the cache, leave it be
					if (val == null) {
						// insert the key/value into the cache
						this.keySet.add(0, key);
						this.cache.setProperty(key, value);
						this.keyCounter += 1;
					}
					// if not in the cache, insert in the front
					break;
					case LFU:
					// LFU
					// Check if it's in the cache already
					val = cache.getProperty(key);
					// if in the cache, just update frequency value
					if (val != null) {
						int index = this.keySet.indexOf(key);
						Integer current = (Integer)this.lfuFreq.get(index);
						this.lfuFreq.set(index, (current + 1));
					// if not in the cache, remove lowest frequency value and insert new value with frequency of 1
					} else {
						// insert new value with frequency of 1
						this.keySet.add(0, key);
						this.lfuFreq.add(0, 1);
						this.cache.setProperty(key, value);
					}
					break;
					case None:
					break;
					default:
					logger.error("Replacement Strategy error: Please ensure proper replacement strategy value");
				}
			}
		}
		
		try (InputStream input = new FileInputStream(fileName)) {
			StatusType status;
			Properties prop = new Properties();

			prop.load(input);
			input.close();

			if ((value == null) || (value == "")) {
				// delete value from key
				Object prev_val = prop.remove(key);
				if (prev_val != null) {
					status = StatusType.DELETE_SUCCESS;
					logger.info("Deleting key-value pair from storage: Key :: " + key + ", Value :: " + prev_val.toString() + "\n");
				} else {
					status = StatusType.DELETE_ERROR;
					logger.info("Cannot delete key-value pair from storage, it doesn't exist" + "\n");
				}
			} else {
				Object prev_val = prop.setProperty(key, value);

				if (prev_val == null) {
					// Didn't exist before
					logger.info("Adding key-value pair to storage: Key :: " + key + ", Value :: " + value + "\n");
					status = StatusType.PUT_SUCCESS;
				} else {
					// Already exists, update instead
					logger.info("Updating key-value pair for Key :: " + key + ", from Value :: " + prev_val.toString()
							+ " to Value :: " + value + "\n");
					status = StatusType.PUT_UPDATE;
				}
			}

			OutputStream output = new FileOutputStream(fileName);
			// store key-values back in file
			prop.store(output, null);
			output.close();

			return status;

		} catch (Exception e) {
			logger.error("IO Failure. \n", e);

			return StatusType.PUT_ERROR;
		}
	}

	public void startSuccessorHandler(String successorServer) {
		// close existing connection
		closeServerConnection();

		//connect to other kvserver
		String[] server_split = successorServer.split(":");
		try {
			connectToServer(server_split[0], Integer.parseInt(server_split[1]));
		} catch (Exception e) {
			logger.error("Error! Unable to connect to successor server.");
		}		
		// start the handler for the successor server
		this.serverMsgHandler = new ServerMessageHandler(successorServerSocket, this);
		new Thread(serverMsgHandler).start();
	}

	public void sendServerMessage(KVMessage msg) {
		//logger.info("in send Server Message");
		if (serverMsgHandler != null && otherServerPublicKey != null)  {
			//logger.info("in send Server Message, handler not null");
			try {
				serverMsgHandler.sendMessage(msg);
			} catch (IOException ioe) {
                logger.error("Error! Unable to send message to other KV server");
            }
		}
	}

	public void closeServerConnection() {
		if (serverMsgHandler != null) {
			logger.info("Closing connection to other KVServer");
			serverMsgHandler.closeConnection();
			serverMsgHandler = null;
			otherServerPublicKey = null;
		}
	}

	public void putReplicas(String key, String value) {
		try {
			// get replicas and make the same put command to them
			Map.Entry<String, String> replica_1_entry = getNextServer(serverHash);
			if (replica_1_entry != null) {
				startSuccessorHandler(replica_1_entry.getValue());
				Thread.sleep(50);
				sendServerMessage(new KVMessage(key, value, StatusType.PUT_REPLICATE));
				if (metadata.size() > 2) {
					Map.Entry<String, String> replica_2_entry = getNextServer(replica_1_entry.getKey());
					startSuccessorHandler(replica_2_entry.getValue());
					Thread.sleep(50);
					sendServerMessage(new KVMessage(key, value, StatusType.PUT_REPLICATE));
				}
			}
		} catch (InterruptedException ioe) {
			logger.error("ERROR, in putReplicas. \n", ioe);
		}
	}


	public String getKvsToTransfer(String successorServer) {
		StringBuilder kv_pairs = new StringBuilder();
		
		//logger.info("successor server is " + successorServer);

		String successorHash = hash(successorServer);
		// transfer
		try (InputStream input = new FileInputStream(fileName)) {

			Properties prop = new Properties();

			// load the kv storage
			prop.load(input);

			Enumeration enu = prop.keys();
			
			while (enu.hasMoreElements()) {
				String key = (String) enu.nextElement();
				String hash_key = hash(key);
				//logger.info("key hash = " + hash_key + ", successorHash = " + successorHash + ", serverHash = " + serverHash);
				
				// either the hash key is larger than the successor's hash, or both the server and successor hashes are larger than the key hash
				// i.e. the key hash is just over 0 on the hash ring, and both the other server are to the left of the 0.
				// see if current server would no llonger be responsible for 
				if (shouldBeTransferred(hash_key, successorHash)) {
					// since the key's hash is greater then the new server's hash
					// it should now belong to the new server
					String value = prop.getProperty(key);
					kv_pairs.append(key + "=" + value + ",");
				}
			}

			if (kv_pairs != null && kv_pairs.length() > 0) {
				kv_pairs.deleteCharAt(kv_pairs.length() - 1);
			}
			
			return kv_pairs.toString();

		} catch (Exception e) {
			logger.error("ERROR in getKvsToTransfer \n", e);
			return null;
		}
	}

	private boolean shouldBeTransferred(String keyHash, String successorHash) {
		TreeSet<String> serverHashes = new TreeSet<>();
		serverHashes.add(successorHash);
		serverHashes.add(serverHash);
		String responsibleHash = serverHashes.ceiling(keyHash);
		if (responsibleHash == null) {
			// both servers have higher hashes, closest going left is the higher of the two
			responsibleHash = serverHashes.first();
		}
		return responsibleHash.equals(successorHash);
	}

	public String getAllKvs() {
		StringBuilder kv_pairs = new StringBuilder();
		
		// transfer
		try (InputStream input = new FileInputStream(fileName)) {

			Properties prop = new Properties();

			// load the kv storage
			prop.load(input);

			Enumeration enu = prop.keys();
			
			while (enu.hasMoreElements()) {
				String key = (String) enu.nextElement();
				String value = prop.getProperty(key);
				kv_pairs.append(key + "=" + value + ",");
			}

			if (kv_pairs != null && kv_pairs.length() > 0) {
				kv_pairs.deleteCharAt(kv_pairs.length() - 1);
			}
			return kv_pairs.toString();

		} catch (Exception e) {
			logger.error("ERROR in getAllKvs. \n", e);
			return null;
		}
	}

	public void insertKvPairs(String kv_pairs) {
		try {
			String[] split_pairs = kv_pairs.split(",");

			for (String kv_pair : split_pairs) {
				String[] split_kv = kv_pair.split("=");
				putKV(split_kv[0], split_kv[1]);
			}
		} catch (Exception e) {
			logger.error("Error in KVServer.insertKvPairs");
		}
	}
	@Override
	public void clearCache() {
		logger.info("Clear Cache. \n");
		if (this.strategy != CacheStrategy.None) {
			// cache.clear();
		}
	}

	@Override
	public void clearStorage() {
		try {
			clearCache();
			logger.info("Clear Storage. \n");
			// storage.clear();
		} catch (Exception e) {
			logger.error("Cannot clear Storage. \n", e);
		}
	}

	public void deleteKvPairs(String kv_pairs) {
		try {
			String[] splitted_pairs = kv_pairs.split(",");
			for (String kv_pair : splitted_pairs) {
				String key = kv_pair.split("=")[0];
				// delete kv_pair
				putKV(key, null);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error in KVServer.deleteKvPairs");
		}
	}

	public void deleteDataFile(){
		File f = new File(fileName);
		f.delete();
	}
	
	@Override
	public void run() {

		running = initializeServer();

		try {
			// ecs_output = ecsSocket.getOutputStream();
			// ecs_input = ecsSocket.getInputStream();

			// try {
			// 	// request metadata from ecs server
			// 	sendMessageToECS(new ECSMessage("placeholder", IECSMessage.StatusType.METADATA));
			// } catch (IOException ioe) {
			// 	logger.error("Error! Connection to ECS lost while trying to get initial metadata!");
			// }

			// new thread to handle incoming ECS messages

			// new Thread(() -> {
			// 	while(isRunning()) {
			// 		try {
			// 			// see if new message from ECS
			// 			ECSMessage latestECSMsg = receiveMessageFromECS();
			// 			handleECSMessage(latestECSMsg);
			// 		} catch (IOException ioe) {
			// 			logger.error("Error! Connection to ECS lost!");
			// 		} catch (Exception e) {
			// 			logger.error("Error! Connection to ECS lost!");
			// 		}
			// 	}
			// }).start();

			if (serverSocket != null) {
				try {
					connectToECS();
				} catch (Exception e) {
					System.out.println("Error! Could not establish connection with ECS Server.");
					e.printStackTrace();
					System.exit(1);
				}
				initializeStorage();
				// start a new thread to handle ECS messages
				ecsHandler = new ECSMessageHandler(ecsSocket, this);
				new Thread(ecsHandler).start();

				while (isRunning()) {
					try {
						Socket client = serverSocket.accept();
						KVClientConnection connection = new KVClientConnection(client, this);
						new Thread(connection).start();

						logger.info("Connected to "
								+ client.getInetAddress().getHostName()
								+ " on port " + client.getPort() + "\n");
					} catch (IOException e) {
						logger.error("Error! " +
								"Unable to establish connection. \n", e);
					}
					// try {
					// 	// see if new message from ECS
					// 	ECSMessage latestECSMsg = receiveMessageFromECS();
					// 	handleECSMessage(latestECSMsg);
					// } catch (IOException ioe) {
					// 	logger.error("Error! Connection to ECS lost!");
					// } catch (Exception e) {
					// 	logger.error("Error! Connection to ECS lost!");
					// }
				}
			}
			logger.info("Server stopped.");
		} catch (Exception e) {
			logger.error("Connection to ecs could not be established.");
		} finally {
			if (isRunning()) {
				//close();
				kill();
			}
		}
	}

	@Override
	public void kill() {
		this.running = false;

		try {
			logger.info("Terminating Server. \n");

			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! Termination failure on port: " + port, e);
		}
	}

	private boolean isRunning() {
		return running;
	}

	private boolean isStopped() {
		return stopped;
	}

	@Override
	public synchronized void close() {
		this.running = false;
		try {
			serverSocket.close();
			ecsSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port + "\n", e);
		}
	}

	public void connectToServer(String server_address, int server_port) throws Exception {
		logger.info("Connecting to KVserver on port " + server_port + "\n");
		this.successorServerSocket = new Socket(server_address, server_port);
	}

	private void connectToECS() throws Exception {
		logger.info("Connecting to ECS on ip: " + this.ecs_addr + " port: " + this.ecs_port + "\n");
		this.ecsSocket = new Socket(this.ecs_addr, this.ecs_port);
		logger.info("Connection to ECS established");
	}

	public void sendEcsMessage(ECSMessage msg) {
		if (ecsHandler != null) {
			try {
				ecsHandler.sendMessageToECS(msg);
			} catch (IOException ioe) {
                logger.error("Error! Unable to send message to ecs server");
            }
		}
	}
	// public void sendMessageToServer(KVMessage msg) throws IOException {
	// 	//byte[] msgBytes = SerializationUtils.serialize(msg);
	// 	byte[] msgBytes = msg.getMsgBytes();
	// 	output.write(msgBytes, 0, msgBytes.length);
	// 	output.flush();
	// 	logger.info("Send message:\t '" + msg.getMsg() + "'");
	// }

	private boolean initializeServer() {
		logger.info("Initialize KVServer ... \n");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("KVServer listening on port: "
					+ serverSocket.getLocalPort() + "\n");
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket: \n");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound! \n");
			}
			return false;
		}
	}

	private void initializeStorage() {
		try {
			File dir = new File(dataDir);
			if(!dir.exists()) {
				dir.mkdirs();
				logger.info("Data directory " + dataDir + " created.\n");
			}
			this.fileName = dataDir + "/" + port + ".properties";
			// see if file exists, if not, create it
			File f = new File(fileName);

			if (f.createNewFile()) {
				logger.info("Created storage file at " + fileName + '\n');
			}
		} catch (IOException e) {
			System.out.println("Error! Unable initialize Storage.");
			e.printStackTrace();
		}
	}

	

	public static void main(String[] args) throws Exception{
		// String ecs_ip_port;
		// String address_str;
		// int port_int;

		// if ((args[0].equals("-a")) && (args[2].equals("-p")) && (args[4].equals("-LL")) && (args[6].equals("-d")) && 
		// (args[8].equals("-s")) && (args[10].equals("-c")) && (args[12].equals("-b"))) {

		// 	address_str = args[3];
		// 	port_int = Integer.parseInt(args[5]);
		// 	ecs_ip_port = args[13];
		// 	try {
		// 		new LogSetup("logs/server" + port_int + ".log", Level.ALL);
		// 	} catch (IOException e) {
		// 	System.out.println("Error! Unable to initialize logger!");
		// 	e.printStackTrace();
		// 	System.exit(1);
		// 	}

		// 	new KVServer(ecs_ip_port, address_str, port_int, 1, "None").start();
		// } else {
		// 	System.out.println("Error! Incorrect arguments. Expected -b <ecs_ip>:<ecs_address> -a <address> -p <port>");
		// }
		Options options = new Options();

		Option ecs_address = new Option("b", "bootstrap", true, "ECS IP address and port <ip>:<port>");
        ecs_address.setRequired(true);
        options.addOption(ecs_address);

        Option address = new Option("a", "address", true, "IP adress");
        address.setRequired(true);
        options.addOption(address);

        Option port_in = new Option("p", "port", true, "Port");
        port_in.setRequired(true);
        options.addOption(port_in);

		Option logLevel = new Option("ll", "logLevel", true, "Log Level. Default is INFO");
        logLevel.setRequired(false);
        options.addOption(logLevel);

		Option dataDir = new Option("d", "dataDir", true, "Directory where data will be stored. Default = data");
        dataDir.setRequired(false);
        options.addOption(dataDir);

		Option strategy = new Option("s", "strategy", true, "Cache Strategy. (Default = None)");
        strategy.setRequired(false);
        options.addOption(strategy);

		Option cacheSize = new Option("c", "cacheSize", true, "Size of cache in kv-pairs. Default is 1.");
        cacheSize.setRequired(false);
        options.addOption(cacheSize);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        HelpFormatter formatter = new HelpFormatter();

        try {

			//default values
			Level log_level = Level.INFO;
			String data_dir = "data";
			CacheStrategy strat = CacheStrategy.None;
			int cache_size = 1;

			cmd = parser.parse(options, args);
			
			if (cmd.hasOption("ll")) {
				log_level = Level.toLevel(cmd.getOptionValue("logLevel"));
			}			
            
			String ecs_ip_port = cmd.getOptionValue("bootstrap");
			String address_str = cmd.getOptionValue("address");
			int port_int = Integer.parseInt(cmd.getOptionValue("port"));

			new LogSetup("logs/server" + port_int + ".log", log_level);

			if (cmd.hasOption("d")) {
				data_dir = cmd.getOptionValue("dataDir");
			}
			if (cmd.hasOption("s")) {
				strat = CacheStrategy.valueOf(cmd.getOptionValue("strategy"));
			}
			if (cmd.hasOption("c")) {
				cache_size = Integer.parseInt(cmd.getOptionValue("cacheSize"));
			}

			new KVServer(ecs_ip_port, address_str, port_int, cache_size, strat, data_dir).start();

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			formatter.printHelp("utility-name", options);
			System.exit(1);
		}
		// } catch (ParseException e) {
		// 	System.out.println(e.getMessage());
		// 	formatter.printHelp("utility-name", options);
		// 	System.exit(1);
		// 	return;
		// }

		
    }
}
