package client;

import shared.messages.KVMessage;
import java.io.IOException;

public interface KVCommInterface {

	/**
	 * Establishes a connection to the KV Server.
	 *
	 * @throws Exception
	 *                   if connection could not be established.
	 */
	public void connect() throws Exception;

	/**
	 * disconnects the client from the currently connected server.
	 */
	public void disconnect();

	public boolean isRunning();

	public void setRunning(boolean run);

	public void addListener(ClientSocketListener listener);

	/**
	 * Method sends a KVMessage using this socket.
	 * 
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(KVMessage msg) throws IOException;

	public KVMessage receiveMessage(boolean encrypted) throws IOException;

	/**
	 * Inserts a key-value pair into the KVServer.
	 *
	 * @param key
	 *              the key that identifies the given value.
	 * @param value
	 *              the value that is indexed by the given key.
	 * @return a message that confirms the insertion of the tuple or an error.
	 * @throws Exception
	 *                   if put command cannot be executed (e.g. not connected to
	 *                   any
	 *                   KV server).
	 */
	public KVMessage put(String key, String value) throws Exception;

	/**
	 * Retrieves the value for a given key from the KVServer.
	 *
	 * @param key
	 *            the key that identifies the value.
	 * @return the value, which is indexed by the given key.
	 * @throws Exception
	 *                   if put command cannot be executed (e.g. not connected to
	 *                   any
	 *                   KV server).
	 */
	public KVMessage get(String key) throws Exception;
}
