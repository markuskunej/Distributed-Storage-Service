package server;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Arrays;


import org.apache.log4j.*;

import app_kvServer.KVServer;

import org.apache.commons.lang3.SerializationUtils;

import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.Crypto;
import java.security.spec.X509EncodedKeySpec;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.security.GeneralSecurityException;

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
	private boolean fromClient = true;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket kvClientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;

	private PublicKey clientPublicKey;
	private PublicKey otherServerPublicKey;

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
					"", StatusType.STRING), false, clientPublicKey);

			// send initial metadata update
			sendMessage(new KVMessage("", kvServer.getMetaData(), StatusType.METADATA), false, clientPublicKey);

			// send server public key to client
			String str_server_pub_key = Base64.getEncoder().encodeToString(kvServer.getPublicKey().getEncoded());
			sendMessage(new KVMessage("", str_server_pub_key, StatusType.PUBLIC_KEY_SERVER), false, clientPublicKey);

			// receive client (or other KVserver) public key
			KVMessage publicKeyMsg = Crypto.receiveEncryptedMessage(input, kvServer.getPrivateKey());
			if (publicKeyMsg.getStatus() == StatusType.PUBLIC_KEY_CLIENT) {
				this.clientPublicKey = Crypto.strToPublicKey(publicKeyMsg.getValue());
				//kvServer.setClientPublicKey(clientPublicKeyMsg.getValue());
			} else if (publicKeyMsg.getStatus() == StatusType.PUBLIC_KEY_SERVER) {
				this.otherServerPublicKey = Crypto.strToPublicKey(publicKeyMsg.getValue());
				//kvServer.setOtherServerPublicKey(clientPub)
			} else {
				logger.error("First message received wasn't a public key, close the server");
				isOpen = false;
			}

			while (isOpen) {
				try {
					KVMessage latestMsg = Crypto.receiveEncryptedMessage(input, kvServer.getPrivateKey());
					KVMessage responseMsg = handleMessage(latestMsg);
					// depending on whether the message was received from a client or server, encrypt with the correct public key
					if (fromClient == true) {
						sendMessage(responseMsg, true, clientPublicKey);
					} else {
						sendMessage(responseMsg, true, otherServerPublicKey);
					}

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
	public void sendMessage(KVMessage msg, boolean encrypt, PublicKey key) throws IOException {
		//byte[] msgBytes = SerializationUtils.serialize(msg);
		byte[] msgBytes = msg.getMsgBytes();

		if (encrypt==true) {
			// Encrypt the KVMessage bytes
			msgBytes = Crypto.encrypt(msgBytes, key);
		}
		/****************************************************************************
		 * In the encrypt call above, serverPublicKey should be the clientPublicKey *
		 * of the client the message is being sent to.								*
		 ****************************************************************************/

		output.write(msgBytes, 0, msgBytes.length); // output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		//logger.info("msgBytes is " + Arrays.toString(msgBytes));
		String logMessage = "";
		if (encrypt==true) {
			logMessage = Base64.getEncoder().encodeToString(msgBytes);
		} else {
			logMessage = msg.getMsg();
		}
		logger.info("SEND \t<"
				+ kvClientSocket.getInetAddress().getHostAddress() + ":"
				+ kvClientSocket.getPort() + ">: '"
				+ logMessage + "'");
	}

	private KVMessage handleMessage(KVMessage msg) throws Exception {
		// need to determine whether message is from client or other kvserver for encryption purposes
		fromClient = true;
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
					returnStatus = kvServer.putKV(msg.getKey(), msg.getValue());
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
			fromClient = false;
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

					returnValue = kvServer.getKV(msg.getKey());
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
			fromClient = false;
			try {
				// Decrypt here too?
				kvServer.insertKvPairs(msg.getKey());

				returnStatus = StatusType.TRANSFER_TO_SUCCESS;		
			} catch (Exception e) {
				logger.error("Error trying insertKvPairs");
				returnStatus = StatusType.TRANSFER_TO_ERROR;
			}
		} else if (msg.getStatus() == StatusType.TRANSFER_ALL_TO) {
			fromClient = false;
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
			sendMessage(new KVMessage("Update metadata and retry", "", StatusType.SERVER_NOT_RESPONSIBLE), true, clientPublicKey);
			sendMessage(new KVMessage("", kvServer.getMetaData(), StatusType.METADATA), true, clientPublicKey);
			// retry same message
			return msg;
		} else {
			// Can leave msg.getKey() and returnValue, as they will either still be encrypted versions
			// sent to the server from the client or newly encrypted from a GET, so the client can decrypt them
			return new KVMessage(msg.getKey(), returnValue, returnStatus);
		}
	}

}
