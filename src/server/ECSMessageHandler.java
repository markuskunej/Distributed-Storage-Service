package server;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import app_kvServer.KVServer;
import shared.messages.ECSMessage;
import shared.messages.KVMessage;
import shared.messages.IECSMessage.StatusType;

public class ECSMessageHandler implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket ecsSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;

	private Thread shutdownHook;

    /**
	 * Constructs a new KVServerConnection object for a given TCP socket.
	 * 
	 * @param ecsSocket the Socket object for the kvServer connection.
	 */
	public ECSMessageHandler(Socket ecsSocket, KVServer kvServer) {
		this.ecsSocket = ecsSocket;
		this.isOpen = true;
		this.kvServer = kvServer;
		shutdownHook = new Thread(new Runnable() {
			@Override
			public void run() {
				shutdown();
			}
		});
		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}

    public void run() {
        try {
            output = ecsSocket.getOutputStream();
            input = ecsSocket.getInputStream();
			// run shutdown hook after opening input and output
			// shutdownHook = new Thread(new Runnable() {
			// 	@Override
			// 	public void run() {
			// 		shutdown();
			// 	}
			// });
			// Runtime.getRuntime().addShutdownHook(shutdownHook);
            //ask for initial metadata
            sendMessageToECS(new ECSMessage(kvServer.getNameServer(), StatusType.NEW_SERVER));

            while (isOpen) {
				try {
					ECSMessage latestECSMsg = receiveMessageFromECS();
					handleECSMessage(latestECSMsg);
				
				} catch (IOException ioe) {
					logger.error("Error! Connection to ECS lost!");
					isOpen = false;
				} catch (Exception e) {
					logger.error("Error! Connection to ECS lost!");
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection to ECS could not be established!", ioe);
		} finally {
			try {
				if (ecsSocket != null) {
					input.close();
					output.close();
					ecsSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection with ECS!", ioe);
			}
		}
    }

    public void sendMessageToECS(ECSMessage msg) throws IOException {
		//byte[] msgBytes = SerializationUtils.serialize(msg);
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message to ECS:\t '" + msg.getMsg() + "'");
	}

	public ECSMessage receiveMessageFromECS() throws IOException {

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
		ECSMessage msg = new ECSMessage(msgBytes);
		logger.info("Receive message from ECS:\t '" + msg.getMsg() + "'");

		return msg;
	}

	private void handleECSMessage(ECSMessage msg) throws Exception {
		if (msg.getStatus() == StatusType.METADATA) {
			try {
				kvServer.setMetaData(msg.getValueAsMetadata());
				if (kvServer.getMetaData() != null) {
					if (kvServer.getMetaData().containsKey(kvServer.getHash())) {
						// KVServer now has metadata and the ECS hash matches the
						// server's hash, no longer in stopped state.
						kvServer.setStopped(false);
					} else {
						logger.error("ERROR! KVServer received a non-null metadata, but doesn't contain it's own hash");
					}
				} else {
					logger.error("ERROR! KVServer received null metadata from ECS!");
				}
			} catch (Exception e) {
				logger.error("Error to read metadata from ECSMessage");
			}
		} else if (msg.getStatus() == StatusType.TRANSFER_TO_REQUEST) {
			kvServer.setWriteLock(true);
			String successorServer = msg.getValue();
			String kv_pairs = kvServer.getKvsToTransfer(successorServer);
			logger.info("KV_PAIRS IS " + kv_pairs);
			if (kv_pairs == null || kv_pairs.length() == 0) {
				logger.info("No KV pairs to transfer");
				sendMessageToECS(new ECSMessage("No KV Pairs needed to be transferred!", StatusType.TRANSFER_TO_REQUEST_SUCCESS));
			} else {
				logger.info("At least 1 KV pair needs to be transferred, connect to successor server");
				kvServer.startSuccessorHandler(successorServer, kv_pairs);
				kvServer.sendServerMessage(new KVMessage(kv_pairs, null, shared.messages.IKVMessage.StatusType.TRANSFER_TO));
			}
		} else if (msg.getStatus() == StatusType.TRANSFER_ALL_TO_REQUEST) {
			String successorServer = msg.getValue();
			String kv_pairs = kvServer.getAllKvs();
			if (kv_pairs == null || kv_pairs.length() == 0) {
				logger.info("No KV pairs to transfer");
				sendMessageToECS(new ECSMessage("No KV Pairs needed to be transferred!", StatusType.TRANSFER_ALL_TO_REQUEST_SUCCESS));
			} else {
				logger.info("At least 1 KV pair needs to be transferred, connect to successor server");
				kvServer.startSuccessorHandler(successorServer, kv_pairs);
				kvServer.sendServerMessage(new KVMessage(kv_pairs, null, shared.messages.IKVMessage.StatusType.TRANSFER_ALL_TO));
			}

		} else if (msg.getStatus() == StatusType.SHUTDOWN_SERVER_SUCCESS) {
			// now it's safe to delete all data + files
			logger.info("before delete data");
			kvServer.deleteDataFile();
			logger.info("after delete data");
			isOpen = false;
		} else if (msg.getStatus() == StatusType.SHUTDOWN_SERVER_ERROR) {
			logger.error("SHUTDOWN_SERVER_ERROR");
		}
		// else if (msg.getStatus() == StatusType.TRANSFER_FROM) {
		// 	kvServer.setWriteLock(true);
		// 	String successorServer = msg.getValue();
		// 	kvServer.transferFromServer(successorServer);
		// 	logger.info("TRANSFER_FROM");
		// }
	}

	private void shutdown() {
		// set write lock of kvserver
		kvServer.setWriteLock(true);
		try {
			// alert ECS of shutdown
			sendMessageToECS(new ECSMessage(kvServer.getNameServer(), StatusType.SHUTDOWN_SERVER));
		} catch (IOException ioe) {
			logger.error("Unable to send shutdown message");
		}
		// wait for final 
		while (isOpen) {
			try {
				logger.info("before receive");
				ECSMessage latestECSMsg = receiveMessageFromECS();
				handleECSMessage(latestECSMsg);
			
			} catch (IOException ioe) {
				ioe.printStackTrace();
				logger.error("Error! Connection to ECS lost before shutdown complete!");
				isOpen = false;
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Error! Connection to ECS lost before shutdown complete!");
				isOpen = false;
			}
		}

		try {
			if (ecsSocket != null) {
				input.close();
				output.close();
				ecsSocket.close();
			}
		} catch (IOException ioe) {
			logger.error("Error! Unable to tear down connection with ECS!", ioe);
		}
	}
}
