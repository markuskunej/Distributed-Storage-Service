package server;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import app_kvServer.KVServer;
import shared.messages.ECSMessage;
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


    /**
	 * Constructs a new KVServerConnection object for a given TCP socket.
	 * 
	 * @param ecsSocket the Socket object for the kvServer connection.
	 */
	public ECSMessageHandler(Socket ecsSocket, KVServer kvServer) {
		this.ecsSocket = ecsSocket;
		this.isOpen = true;
		this.kvServer = kvServer; 
	}

    public void run() {
        try {
            output = ecsSocket.getOutputStream();
            input = ecsSocket.getInputStream();
            //ask for initial metadata
            sendMessageToECS(new ECSMessage(kvServer.getNameServer(), StatusType.NEW_SERVER));

            while (isOpen) {
                ECSMessage latestECSMsg = receiveMessageFromECS();
                handleECSMessage(latestECSMsg);
            }
        } catch (IOException ioe) {
            logger.error("Error! Connection to ECS lost!");
            isOpen = false;
        } catch (Exception e) {
            logger.error("Error! Connection to ECS lost!");
            isOpen = false;
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
		} else if (msg.getStatus() == StatusType.TRANSFER_FROM) {
			logger.info("TRANSFER_FROM");
		}
	}
}
