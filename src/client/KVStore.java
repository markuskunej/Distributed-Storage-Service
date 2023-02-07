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
import java.util.Set;

import org.apache.log4j.Logger;

import client.ClientSocketListener.SocketStatus;

public class KVStore extends Thread implements Serializable, KVCommInterface {

	private String address;
	private int port;
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private Socket kvStoreSocket;
	private OutputStream output;
	private InputStream input;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		try {
			this.connect();
		} catch (Exception e) {
			logger.error("Unable to connect");
		}
	}

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = kvStoreSocket.getOutputStream();
			input = kvStoreSocket.getInputStream();

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
			// input.close();
			// output.close();
			kvStoreSocket.close();
			kvStoreSocket = null;
			logger.info("connection closed!");
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public void setRunning(boolean run) {
		running = run;
	}

	@Override
	public void addListener(ClientSocketListener listener) {
		listeners.add(listener);
	}

	@Override
	public void sendMessage(KVMessage msg) throws IOException {
		byte[] msgBytes = SerializationUtils.serialize(msg);
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
	}

	@Override
	public KVMessage receiveMessage() throws IOException {
		return (KVMessage) SerializationUtils.deserialize(input);
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		sendMessage(new KVMessage(key, value, StatusType.PUT));
		return receiveMessage();
	}

	@Override
	public KVMessage get(String key) throws Exception {
		sendMessage(new KVMessage(key, null, StatusType.GET));
		return receiveMessage();
	}
}
