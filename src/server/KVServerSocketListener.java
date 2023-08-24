package server;

import shared.messages.KVMessage;

public interface KVServerSocketListener {

	public enum SocketStatus {
		CONNECTED, DISCONNECTED, CONNECTION_LOST
	};

	public void handleNewMessage(KVMessage msg);

	public void handleStatus(SocketStatus status);
}
