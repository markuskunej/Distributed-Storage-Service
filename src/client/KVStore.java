package client;

import java.io.Serializable;
import org.apache.commons.lang3.SerializationUtils;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;

import java.util.Map;
import java.util.TreeMap;
import java.math.BigInteger;

import org.apache.commons.codec.digest.DigestUtils;

import org.apache.log4j.Logger;

import client.ClientSocketListener.SocketStatus;

public class KVStore extends Thread implements Serializable, KVCommInterface {

	private String address;
	private int port;
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;
	private boolean streamsOpen;

	private Socket kvStoreSocket;
	private OutputStream output;
	private InputStream input;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	private TreeMap<String, String> metaData = new TreeMap<String, String>();

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.streamsOpen = false;
	}

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = kvStoreSocket.getOutputStream();
			input = kvStoreSocket.getInputStream();
			streamsOpen = true;
			while (isRunning()) {
				try {
					KVMessage latestMsg = receiveMessage();
					for (ClientSocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (IOException ioe) {
					if (isRunning()) {
						logger.error("Connection lost!");
						try {
							tearDownConnection();
							for (ClientSocketListener listener : listeners) {
								listener.handleStatus(
										SocketStatus.CONNECTION_LOST);
							}
						} catch (IOException e) {
							logger.error("Unable to close connection!");
						}
					}
				}
			}
		} catch (IOException ioe) {
			logger.error("Connection could not be established!");

		} finally {
			if (isRunning()) {
				disconnect();
			}
		}
	}

	@Override
	public void connect() throws Exception {
		kvStoreSocket = new Socket(address, port);
		listeners = new HashSet<ClientSocketListener>();
		setRunning(true);
		logger.info("Connection established");
	}

	@Override
	public void disconnect() {
		logger.info("try to close connection ...");
		try {
			tearDownConnection();
			for (ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (kvStoreSocket != null) {
			//input.close();
			//output.close();
			kvStoreSocket.close();
			kvStoreSocket = null;
			logger.info("connection closed!");
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	public boolean areStreamsOpen() {
		return streamsOpen;
	}

	@Override
	public void setRunning(boolean run) {
		running = run;
	}

	public void setMetaData(TreeMap<String, String> metaData) {
		this.metaData = metaData;
	}

	@Override
	public void addListener(ClientSocketListener listener) {
		listeners.add(listener);
	}

	@Override
	public void sendMessage(KVMessage msg) throws IOException {
		//byte[] msgBytes = SerializationUtils.serialize(msg);
		byte[] msgBytes = msg.getMsgBytes();
		logger.debug("msgBytes = null is " + (msgBytes == null));
		logger.debug("output = null is " + (output == null));
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getMsg() + "'");
	}

	@Override
	public KVMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();

		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		//KVMessage receivedMsg = (KVMessage) SerializationUtils.deserialize(msgBytes);
		KVMessage msg = new KVMessage(msgBytes);
		logger.info("Receive message:\t '" + msg.getMsg() + "'");

		return msg;
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		KVMessage msg = new KVMessage(key, value, StatusType.PUT);
		sendMessage(msg);

		return msg;
	}

	@Override
	public KVMessage get(String key) throws Exception {		
		KVMessage msg = new KVMessage(key.trim(), "", StatusType.GET);
		logger.debug("msg is " + msg.getMsg());
		sendMessage(msg);

		return msg;
	}

    public boolean connected() {
	try{
			// Note:
			// getInetAddress() returns the address to which the socket is connected
			// isReachable() checks if the address is reachable, times out after 10 milliseconds
	    if (kvStoreSocket != null && kvStoreSocket.getInetAddress().isReachable(10))
		return true;
	    else
		return false;
	}
	catch (IOException ioe){
	    return false;
	}
    }

	public void keyrange() throws Exception {
		KVMessage msg = new KVMessage("1", "1", StatusType.KEYRANGE);
		sendMessage(msg);
	}
	
	public String getResponsible(String key) {
		try {
			if (metaData != null) {
				String hash = DigestUtils.md5Hex(key);
				// Note: EntrySet() returns a set of the same elements already present in the hash map
				Map.Entry<String, String> server = metaData.floorEntry(hash);
				if (server == null) {
					server = metaData.lastEntry();
				}

				String serverValue = server.getValue();

				return serverValue;
				// String parts[] = serverValue.split(":");
				// String metaAddress = parts[0];
				// String metaPort = parts[1];

				// this.address = metaAddress;
				// this.port = Integer.parseInt(metaPort);

			} else {
				logger.error("Recent metadata could not be retreived");
				return "";
			}
		} catch (Exception e) {
			logger.error("Responsible server" + e);
			return "";
		}
	}

	public void retryOperation(KVMessage msg) throws Exception {
		// disconnect from current server
		disconnect();
		// find responsible server according to metadata and set address and port to it
		getResponsible(msg.getKey());
		// connect to new server
		try {
			connect();
		} catch (Exception e) {
			logger.error("Unable to connect to responsible server according to metadata, trying other servers until connected.");
			connectOnLoss();
		}
		sendMessage(msg);
	}

	private void connectOnLoss() {
		// If a connection is lost, try to reconnect with appropriate metadata
		try {
			// loop while not connected
			while(!connected()) {
				for (String meta : metaData.values()) {
					// split by semicolon, get address and port
					String parts[] = meta.split(":");
					String metaAddress = parts[0];
					String metaPort = parts[1];
					if (Integer.parseInt(metaPort) != this.port) {
						this.port = Integer.parseInt(metaPort);
						this.address = metaAddress;
						disconnect();
						connect();
					}
				}
			}
		} catch (Exception e) {
			logger.error("Server connection failure");
		}
	}
}
