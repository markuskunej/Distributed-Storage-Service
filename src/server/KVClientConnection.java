package server;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import app_kvServer.KVServer;

import org.apache.commons.lang3.SerializationUtils;

import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

import java.security.GeneralSecurityException;
// Encryption Imports
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import javax.crypto.Cipher;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class KVClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket kvClientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;

	private static PrivateKey serverPrivateKey;
	private static PublicKey serverPublicKey;

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
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 */
	public KVClientConnection(Socket clientSocket, KVServer server) {
		this.kvClientSocket = clientSocket;
		this.isOpen = true;
		this.kvServer = server;
	}

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = kvClientSocket.getOutputStream();
			input = kvClientSocket.getInputStream();	

			sendMessage(new KVMessage(
					"Connection to KVServer established: "
							+ kvClientSocket.getLocalAddress() + " / "
							+ kvClientSocket.getLocalPort(),
					"", StatusType.STRING));

			// send initial metadata update
			sendMessage(new KVMessage("", kvServer.getMetaData(), StatusType.METADATA));

			while (isOpen) {
				try {
					KVMessage latestMsg = receiveMessage();
					KVMessage responseMsg = handleMessage(latestMsg);
					sendMessage(responseMsg);

					/*
					 * connection either terminated by the client or lost due to
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
				if (kvClientSocket != null) {
					input.close();
					output.close();
					kvClientSocket.close();
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
	public void sendMessage(KVMessage msg) throws IOException {
		//byte[] msgBytes = SerializationUtils.serialize(msg);
		byte[] msgBytes = msg.getMsgBytes();

		// Encrypt the KVMessage bytes
		byte[] encryptedData = encrypt(msgBytes, serverPublicKey);

		/****************************************************************************
		 * In the encrypt call above, serverPublicKey should be the clientPublicKey *
		 * of the client the message is being sent to.								*
		 ****************************************************************************/

		output.write(encryptedData, 0, encryptedData.length); // output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<"
				+ kvClientSocket.getInetAddress().getHostAddress() + ":"
				+ kvClientSocket.getPort() + ">: '"
				+ msg.getMsg() + "'");
	}

	private KVMessage receiveMessage() throws IOException {

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

		// Decrypt here
		byte[] decryptedBytes = decrypt(msgBytes, serverPrivateKey);

		/***********************************************************************
		 * In the decrypt call above, serverPrivateKey is correct - the server *
		 * decrypts the data with its own private key	   					   *
		 ***********************************************************************/

		//KVMessage receivedMsg = (KVMessage) SerializationUtils.deserialize(msgBytes);
		KVMessage receivedMsg = new KVMessage(decryptedBytes); // KVMessage receivedMsg = new KVMessage(msgBytes);

		/* build final String */
		logger.info("RECEIVE \t<"
				+ kvClientSocket.getInetAddress().getHostAddress() + ":"
				+ kvClientSocket.getPort() + ">: '"
				+ receivedMsg.getMsg().trim() + "'");
		return receivedMsg;
	}

	public byte[] encrypt(byte[] data, PublicKey publicKey) {
		Cipher cipher = null;
		byte[] encrypted;

		try {
			cipher = Cipher.getInstance("RSA");
			cipher.init(Cipher.ENCRYPT_MODE, serverPublicKey, new SecureRandom());

			encrypted = cipher.doFinal(data);
		} catch (GeneralSecurityException e) {
			throw new RuntimeException(e);
			// gracceful exit
		}
    	
		return encrypted;
	}

	public byte[] decrypt(byte[] encryptedData, PrivateKey privateKey) {
		Cipher cipher = null;
		byte[] decrypted;

		try {
			cipher = Cipher.getInstance("RSA");
			cipher.init(Cipher.DECRYPT_MODE, serverPrivateKey);
	
			decrypted = cipher.doFinal(encryptedData);
		} catch (GeneralSecurityException e) {
			throw new RuntimeException(e);
			// graceful exit
		}
    	
		return decrypted;
	}

	private KVMessage handleMessage(KVMessage msg) throws Exception {
		String returnValue = msg.getValue();
		StatusType returnStatus = msg.getStatus();
		if (msg.getStatus() == StatusType.PUT) {
			if (kvServer.isResponsible(msg.getKey(), true)) {
				try {
					// Decrypt key and value first, then run putKV
					// String decryptedKey = decrypt(msg.getKey(), serverPrivateKey);
					// String decryptedValue = decrypt(msg.getValue(), serverPrivateKey);

					// Note that we need to re-encrypt the value returned by getKV before sending it to the client
					// returnStatus = encrypt(kvServer.putKV(decryptedKey, decryptedValue), this.serverPublicKey);
					if (returnStatus != StatusType.PUT_ERROR) {
						kvServer.putReplicas(msg.getKey(), msg.getValue());
					}
				} catch (Exception e) {
					logger.error("Error trying putKV");
					returnStatus = StatusType.PUT_ERROR;
				}
			} else {
				returnStatus = StatusType.SERVER_NOT_RESPONSIBLE;
			}
		} else if (msg.getStatus() == StatusType.PUT_REPLICATE) {
			try {
				// Decrypt here too?
				returnStatus = kvServer.putKV(msg.getKey(), msg.getValue());
			} catch (Exception e) {
				logger.error("Error trying putKV");
				returnStatus = StatusType.PUT_ERROR;
			}
		} else if (msg.getStatus() == StatusType.GET) {
			if (kvServer.isResponsible(msg.getKey(), false)) {
				try {
					// Decrypt key first, then run getKV
					// String decryptedKey = decrypt(msg.getKey(), this.serverPrivateKey);

					// returnValue = kvServer.getKV(decryptedKey);
					if (returnValue != null) {
						returnStatus = StatusType.GET_SUCCESS;
					} else {
						returnStatus = StatusType.GET_ERROR;
					}
					
				} catch (Exception e) {
					logger.error("Error trying getKV");
					returnStatus = StatusType.GET_ERROR;
				}
			} else {
				returnStatus = StatusType.SERVER_NOT_RESPONSIBLE;
			}
		} else if (msg.getStatus() == StatusType.TRANSFER_TO) {
			try {
				// Decrypt here too?
				kvServer.insertKvPairs(msg.getKey());

				returnStatus = StatusType.TRANSFER_TO_SUCCESS;		
			} catch (Exception e) {
				logger.error("Error trying insertKvPairs");
				returnStatus = StatusType.TRANSFER_TO_ERROR;
			}
		} else if (msg.getStatus() == StatusType.TRANSFER_ALL_TO) {
			try {
				// Decrypt here too?

				//logger.info("kv's to add are " + msg.getKey());
				kvServer.insertKvPairs(msg.getKey());

				returnStatus = StatusType.TRANSFER_ALL_TO_SUCCESS;		
			} catch (Exception e) {
				logger.error("Error trying insertKvPairs");
				returnStatus = StatusType.TRANSFER_ALL_TO_ERROR;
			}
		} else if (msg.getStatus() == StatusType.KEYRANGE) {
			try {
				returnValue = kvServer.getKeyrange();
				returnStatus = StatusType.KEYRANGE_SUCCESS;
			} catch (Exception e) {
				logger.error("Error in getKeyrange");
				returnStatus = StatusType.KEYRANGE_ERROR;
			}
		} else if (msg.getStatus() == StatusType.KEYRANGE_READ) {
			try {
				returnValue = kvServer.getKeyrangeRead();
				returnStatus = StatusType.KEYRANGE_READ_SUCCESS;
			} catch (Exception e) {
				logger.error("Error in getKeyrangeRead");
				returnStatus = StatusType.KEYRANGE_READ_ERROR;
			}
		}

		if (returnStatus == StatusType.SERVER_NOT_RESPONSIBLE) {
			// send metaupdate to kvstore first
			sendMessage(new KVMessage("Update metadata and retry", "", StatusType.SERVER_NOT_RESPONSIBLE));
			sendMessage(new KVMessage("", kvServer.getMetaData(), StatusType.METADATA));
			// retry same message
			return msg;
		} else {
			// Can leave msg.getKey() and returnValue, as they will either still be encrypted versions
			// sent to the server from the client or newly encrypted from a GET, so the client can decrypt them 
			return new KVMessage(msg.getKey(), returnValue, returnStatus);
		}
	}

}
