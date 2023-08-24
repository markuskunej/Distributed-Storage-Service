package shared.messages;

public interface IECSMessage {

	public enum StatusType {
		METADATA,
		METADATA_SUCCESS,
		METADATA_ERROR,
		TRANSFER_FROM,
		TRANSFER_TO_REQUEST,
		TRANSFER_TO_REQUEST_SUCCESS,
		TRANSFER_TO_REQUEST_ERROR,
		TRANSFER_ALL_TO_REQUEST,
		TRANSFER_ALL_TO_REQUEST_SUCCESS,
		TRANSFER_ALL_TO_REQUEST_ERROR,
		REBALANCE,
		REBALANCE_SUCCESS,
		REBALANCE_ERROR,
		NEW_SERVER,
		NEW_SERVER_SUCCESS,
		NEW_SERVER_ERROR,
		SAFE_TO_DELETE,
		SHUTDOWN_SERVER,
		SHUTDOWN_SERVER_SUCCESS,
		SHUTDOWN_SERVER_ERROR,
		STRING /* pass text using the key param */
	}

	/**
	 * @return the key that is associated with this message,
	 *         null if not key is associated.
	 */
	public String getMsg();


	/**
	 * @return a status string that is used to identify request types,
	 *         response types and error types associated to the message.
	 */
	public StatusType getStatus();

}
