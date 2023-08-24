package shared.messages;

public interface IKVMessage {

	public enum StatusType {
		GET, /* Get - request */
		GET_ERROR, /* requested tuple (i.e. value) not found */
		GET_SUCCESS, /* requested tuple (i.e. value) found */
		PUT, /* Put - request */
		PUT_REPLICATE, /* Replicated put request from the coordinator */
		PUT_SUCCESS, /* Put - request successful, tuple inserted */
		PUT_UPDATE, /* Put - request successful, i.e. value updated */
		PUT_ERROR, /* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, /* Delete - request successful */
		KEYRANGE,
		KEYRANGE_SUCCESS,
		KEYRANGE_ERROR,
		KEYRANGE_READ,
		KEYRANGE_READ_SUCCESS,
		KEYRANGE_READ_ERROR,
		STRING, /* pass text using the key param */
		INITIAL,
		METADATA,
		PUBLIC_KEY_CLIENT,
		PUBLIC_KEY_CLIENT_SUCCESS,
		PUBLIC_KEY_SERVER,
		TRANSFER_TO,
		TRANSFER_TO_SUCCESS,
		TRANSFER_TO_ERROR,
		TRANSFER_ALL_TO,
		TRANSFER_ALL_TO_SUCCESS,
		TRANSFER_ALL_TO_ERROR,
		SERVER_STOPPED,
		SERVER_WRITE_LOCK,
		SERVER_NOT_RESPONSIBLE
	}

	/**
	 * @return the key that is associated with this message,
	 *         null if not key is associated.
	 */
	public String getKey();

	/**
	 * @return the value that is associated with this message,
	 *         null if not value is associated.
	 */
	public String getValue();

	/**
	 * @return a status string that is used to identify request types,
	 *         response types and error types associated to the message.
	 */
	public StatusType getStatus();

}
