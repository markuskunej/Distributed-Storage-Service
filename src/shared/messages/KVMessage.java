package shared.messages;

import java.io.Serializable;

public class KVMessage implements Serializable, IKVMessage {
	private StatusType status;
	private String key;
	private String value;

	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	public KVMessage(String k, String v, StatusType st) {
		this.key = k;
		this.value = v;
		this.status = st;
	}

	@Override
	public String getKey() {
		return key.trim();
	}

	@Override
	public String getValue() {
		return value.trim();
	}

	@Override
	public StatusType getStatus() {
		return status;
	}

	public String getMsg() {
		return this.key + " " + this.value + " " + this.status.name();
	}
}
