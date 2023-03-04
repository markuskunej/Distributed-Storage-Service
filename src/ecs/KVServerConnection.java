package ecs;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.TreeMap;

import org.apache.log4j.*;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;

import org.apache.commons.lang3.SerializationUtils;

import shared.messages.IECSMessage;
import shared.messages.ECSMessage;
import shared.messages.IECSMessage.StatusType;

/**
 * Represents a connection end point for a particular KVServer that is
 * connected to the ECS. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the KVServer.
 */
public class KVServerConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket kvServerSocket;
	private InputStream input;
	private OutputStream output;
	private ECSClient ecsServer;

	/**
	 * Constructs a new KVServerConnection object for a given TCP socket.
	 * 
	 * @param kvServerSocket the Socket object for the kvServer connection.
	 */
	public KVServerConnection(Socket kvServerSocket, ECSClient ecsClient) {
		this.kvServerSocket = kvServerSocket;
		this.isOpen = true;
		this.ecsServer = ecsClient; 
	}

	/**
	 * Initializes and starts the kvServer connection.
	 * Loops until the connection is closed or aborted by the KVServer.
	 */
	public void run() {
		try {
			output = kvServerSocket.getOutputStream();
			input = kvServerSocket.getInputStream();

			sendMessage(new ECSMessage(
					"Connection to KVServer established: "
							+ kvServerSocket.getLocalAddress() + " / "
							+ kvServerSocket.getLocalPort(), StatusType.STRING));

		
			while (isOpen) {
				try {
					ECSMessage latestMsg = receiveMessage();
					ECSMessage responseMsg = handleMessage(latestMsg);
					sendMessage(responseMsg);

					/*
					 * connection either terminated by the ECS or lost due to
					 * network problems
					 */
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				} catch (Exception e) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {

			try {
				if (kvServerSocket != null) {
					input.close();
					output.close();
					kvServerSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	/**
	 * Method sends a KVMessage using this socket.
	 * 
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(ECSMessage msg) throws IOException {
		//byte[] msgBytes = SerializationUtils.serialize(msg);
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<"
				+ kvServerSocket.getInetAddress().getHostAddress() + ":"
				+ kvServerSocket.getPort() + ">: '"
				+ msg.getMsg() + "'");
	}

	private ECSMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];


		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;


		while (/* read != 13 && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
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

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

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
		ECSMessage receivedMsg = new ECSMessage(msgBytes);

		/* build final String */
		logger.info("RECEIVE \t<"
				+ kvServerSocket.getInetAddress().getHostAddress() + ":"
				+ kvServerSocket.getPort() + ">: '"
				+ receivedMsg.getMsg().trim() + "'");
		return receivedMsg;
	}

	private ECSMessage handleMessage(ECSMessage msg) throws Exception {
		ECSMessage returnMsg = new ECSMessage("ERROR IN HANDLE_MESSAGE", StatusType.STRING);
		if (msg.getStatus() == StatusType.NEW_SERVER) {
			try {
				String server_name = msg.getValue();
				ecsServer.addToMetaData(server_name);
				ECSMessage metadata_update = new ECSMessage(ecsServer.getMetaData(), StatusType.METADATA);
				sendMessage(metadata_update);

				String successorName = ecsServer.getSuccesorServer(server_name);
				if (successorName != null) {
					ECSMessage rebalance_msg = new ECSMessage(successorName, StatusType.TRANSFER_FROM);
					sendMessage(rebalance_msg);
				}

				returnMsg = new ECSMessage("", StatusType.NEW_SERVER_SUCCESS);
				//TreeMap<String, String> metadata = ecsServer.getMetaData();
				// if (metadata != null) {
				// 	returnMsg = new ECSMessage(metadata, StatusType.METADATA_SUCCESS);
				// } else {
				// 	logger.error("getMetaData produced empty tree!");
				// 	returnMsg = new ECSMessage("", StatusType.METADATA_ERROR);
				// }
			} catch (Exception e) {
				logger.error("Error adding new server");
				e.printStackTrace();
				returnMsg = new ECSMessage("", StatusType.NEW_SERVER_ERROR);
			}
		}

		return returnMsg;

	}

}
