package shared.messages;

import java.io.Serializable;
import java.util.TreeMap;

public class KVMessage implements Serializable, IKVMessage {
	private static final long serialVersionUID = 5549512212003782618L;
	private StatusType status;
	private String key;
	private String value;
	private String msg;
	private byte[] msgBytes;

	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	public KVMessage(String k, String v, StatusType st) {
		this.key = k;
		this.value = v;
		this.status = st;
		this.msg = buildMsg();
		this.msgBytes = toByteArray(msg);
	}

	public KVMessage(String k, TreeMap<String, String> metadata, StatusType st) {
		this.key = k;
		this.value = treeToStr(metadata);
		this.status = st;
		this.msg = buildMsg();
		this.msgBytes = toByteArray(msg);
	}

	/**
	 * Constructs a KVMessage object with a given array of bytes that
	 * forms the message.
	 * 
	 * @param bytes the bytes that form the message in ASCII coding.
	 */
	public KVMessage(byte[] bytes) {
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(msgBytes).trim();
		setKV(msg);
	}

	/**
	 * Constructs a KVMessage object with a given String that
	 * forms the message.
	 * 
	 * @param msg the String that forms the message.
	 */
	public KVMessage(String msg) {
		this.msg = msg;
		setKV(msg);
		this.msgBytes = toByteArray(msg);
	}

	@Override
	public String getKey() {
		return key.trim();
	}

	@Override
	public String getValue() {
		if (value != null) {
			return value.trim();
		} else {
			return null;
		}
	}

	@Override
	public StatusType getStatus() {
		return status;
	}

	/**
	 * Returns the content of this KVMessage as a String.
	 * 
	 * @return the content of this message in String format.
	 */
	public String getMsg() {
		return msg.trim();
	}

	/**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 * 
	 * @return the content of this message as an array of bytes
	 *         in ASCII coding.
	 */
	public byte[] getMsgBytes() {
		return msgBytes;
	}

	private void setKV(String msg) {
		String[] splitted = msg.split("~");
		if (splitted.length == 2) {
			this.key = splitted[0].trim();
			this.value = "";
			this.status = StatusType.valueOf(splitted[1].trim());
		} else if (splitted.length == 3) {
			this.key = splitted[0].trim();
			this.value = splitted[1].trim();
			this.status = StatusType.valueOf(splitted[2].trim());
		}
	}

	private String treeToStr(TreeMap<String, String> tree) {
		StringBuilder treeAsString = new StringBuilder();
		for (String hash : tree.keySet()) {
			treeAsString.append(hash + "=" + tree.get(hash) + ",");
		}
		// delete , at end
		treeAsString.deleteCharAt(treeAsString.length() -1);

		return treeAsString.toString();
	}

	public TreeMap<String, String> getValueAsMetadata() {
		TreeMap<String, String> metadata = new TreeMap<String, String>();
		String[] entries_arr = value.split(",");
		for (String entry_str: entries_arr) {
			String[] entry_split = entry_str.split("=");
			metadata.put(entry_split[0], entry_split[1]);
		}
		
		return metadata;
	}

	private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
		byte[] tmp = new byte[bytes.length + ctrBytes.length];

		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

		return tmp;
	}

	private byte[] toByteArray(String s) {
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
		byte[] tmp = new byte[bytes.length + ctrBytes.length];

		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

		return tmp;
	}

	private String buildMsg() {
		if ((value != null) && (value != "")) {
			return key.trim() + "~" + value.trim() + "~" + status.name();
		} else {
			return key.trim() + "~" + status.name();
		}
	}
}
