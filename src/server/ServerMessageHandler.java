package server;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Base64;

import org.apache.log4j.*;

import app_kvServer.KVServer;
import shared.messages.ECSMessage;
import shared.messages.IECSMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.Crypto;

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

public class ServerMessageHandler implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket successorSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;

	private PublicKey otherServerPublicKey;

    /**
	 * Constructs a new KVServerConnection object for a given TCP socket.
	 * 
	 * @param successorSocket the Socket object for the kvServer connection.
	 */
	public ServerMessageHandler(Socket successorSocket, KVServer kvServer) {
		this.successorSocket = successorSocket;
		this.isOpen = true;
		this.kvServer = kvServer;
	}

    public void run() {
        try {
            output = successorSocket.getOutputStream();
            input = successorSocket.getInputStream();

			// sendMessage(new KVMessage(
			// 		"Connection to successor Server established: "
			// 				+ successorSocket.getLocalAddress() + " / "
			// 				+ successorSocket.getLocalPort(), null, StatusType.STRING));

			//sendMessage(new KVMessage(pairs_to_transfer, null, StatusType.TRANSFER_TO));

            while (isOpen) {
				try {
					KVMessage latestMsg;
					if (otherServerPublicKey != null) {
						latestMsg = Crypto.receiveEncryptedMessage(input, kvServer.getPrivateKey());
					} else {
						latestMsg = receiveMessage();
					}
					handleMessage(latestMsg);
            
				} catch (IOException ioe) {
					logger.error("Error! Connection to successor KVServer lost!");
					isOpen = false;
				} catch (Exception e) {
					logger.error("Error! Connection to successor KVServer lost!");
					isOpen = false;
				}
			}
		} catch (IOException ioe) {
			logger.error("Error! Connection to successor KVServer could not be established!", ioe);
		} finally {
			try {
				if (successorSocket != null) {
					logger.info("TRYING TO CLOSE KVSERVER");
					input.close();
					output.close();
					successorSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection with successor KVServer!", ioe);
			}
		}
    }

	public void closeConnection() {
		this.isOpen = false;
	}

    public void sendMessage(KVMessage msg) throws IOException {
		//byte[] msgBytes = SerializationUtils.serialize(msg);
		byte[] msgBytes = msg.getMsgBytes();
		if (otherServerPublicKey != null) {
			// Encrypt the KVMessage bytes
			msgBytes = Crypto.encrypt(msgBytes, otherServerPublicKey); // serverPublicKey
		}
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message to successor KVServer:\t '" + msg.getMsg() + "'");
	}

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

		// if (encrypted==true) {
		// 	// Decrypt here
		// 	msgBytes = decrypt(msgBytes, kvServer.getPrivateKey());
		// }

		//KVMessage receivedMsg = (KVMessage) SerializationUtils.deserialize(msgBytes);
		KVMessage msg = new KVMessage(msgBytes);
		logger.info("Receive message from successor KVServer:\t '" + msg.getMsg() + "'");

		return msg;
	}

	// private PublicKey strToPublicKey (String key) {
	// 	try {
	// 		X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(key.getBytes());
	// 		KeyFactory kf = KeyFactory.getInstance("RSA/ECB/PKCS1Padding");

	// 		return kf.generatePublic(X509publicKey);
	// 	} catch (Exception e) {
	// 		e.printStackTrace();
	// 		return null;
	// 	}
	// }

	private void handleMessage(KVMessage msg) throws Exception {
		if (msg.getStatus() == StatusType.PUBLIC_KEY_SERVER) {
			this.otherServerPublicKey = Crypto.strToPublicKey(msg.getValue());
			logger.info("Received public key from connected KVServer, respond with its own public key");
			String str_server_pub_key = Base64.getEncoder().encodeToString(kvServer.getPublicKey().getEncoded());			
			sendMessage(new KVMessage("", str_server_pub_key, StatusType.PUBLIC_KEY_SERVER));
		} else if (msg.getStatus() == StatusType.TRANSFER_TO_SUCCESS) {
			logger.info("Succesfully transferred kv pairs to successor KVServer.");
			logger.info("Closing connection with successor KVServer.");
			kvServer.sendEcsMessage(new ECSMessage(msg.getKey(), IECSMessage.StatusType.TRANSFER_TO_REQUEST_SUCCESS));
			// close successor server connection
			closeConnection();
		} else if (msg.getStatus() == StatusType.TRANSFER_TO_ERROR) {
			logger.info("TRANSFER_TO_ERROR");
			logger.info("Closing connection with successor KVServer.");
			kvServer.sendEcsMessage(new ECSMessage("ERROR! Unable to transfer kv_pairs between servers.", IECSMessage.StatusType.TRANSFER_TO_REQUEST_ERROR));
			closeConnection();
		} else if (msg.getStatus() == StatusType.TRANSFER_ALL_TO_SUCCESS) {
			logger.info("Successfully transferred all KV Pairs from the server to be shut down");
			logger.info("Closing connection with successor KVServer.");
			kvServer.sendEcsMessage(new ECSMessage("Successfully transferred all kv pairs from server.", IECSMessage.StatusType.TRANSFER_ALL_TO_REQUEST_SUCCESS));
			closeConnection();
		} else if (msg.getStatus() == StatusType.TRANSFER_ALL_TO_ERROR) {
			logger.info("TRANSFER_ALL_TO_ERROR");
			logger.info("Closing connection with successor KVServer.");
			kvServer.sendEcsMessage(new ECSMessage("ERROR! Unable to transfer all kv_pairs between servers.", IECSMessage.StatusType.TRANSFER_ALL_TO_REQUEST_ERROR));
			closeConnection();
		} else if (msg.getStatus() == StatusType.STRING || msg.getStatus() == StatusType.METADATA) {
			//ignore these
		} else {
			logger.info(msg.getStatus() + " with other KVServer, close the connection");
			closeConnection();
		}
	}

	// 	public byte[] encrypt(byte[] data, PublicKey publicKey) {
	// 	Cipher cipher = null;
	// 	byte[] encrypted;

	// 	try {
	// 		cipher = Cipher.getInstance("RSA");
	// 		cipher.init(Cipher.ENCRYPT_MODE, publicKey, new SecureRandom());
	// 		encrypted = cipher.doFinal(data);
	// 	} catch (GeneralSecurityException e) {
	// 		throw new RuntimeException(e);
	// 		// graceful exit
	// 	}
    	
	// 	return encrypted;
	// }

	// public byte[] decrypt(byte[] encryptedData, PrivateKey privateKey) {
	// 	Cipher cipher = null;
	// 	byte[] decrypted;

	// 	try {
	// 		cipher = Cipher.getInstance("RSA");
    // 		cipher.init(Cipher.DECRYPT_MODE, privateKey);

	// 		decrypted = cipher.doFinal(encryptedData);
	// 	} catch (GeneralSecurityException e) {
	// 		throw new RuntimeException(e);
	// 		// graceful exit
	// 	}
    	
	// 	return decrypted;
	// }
}
