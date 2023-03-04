package shared.messages;

import java.io.Serializable;
import java.util.TreeMap;

public class ECSMessage implements Serializable, IECSMessage {
	private static final long serialVersionUID = 5549512212003782618L;
	private StatusType status;
	private String value;
	private String msg;
	private byte[] msgBytes;

	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	public ECSMessage(String value, StatusType st) {
		this.status = st;
		this.value = value;
		this.msg = buildMsg();
		this.msgBytes = toByteArray(msg);
	}

	/**
	 * Constructs a ECSMessage object with a given array of bytes that
	 * forms the message.
	 * 
	 * @param bytes the bytes that form the message in ASCII coding.
	 */
	public ECSMessage(byte[] bytes) {
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(msgBytes).trim();
		setECS(msg);
	}

	/**
	 * Constructs a ECSMessage object with a given String that
	 * forms the message.
	 * 
	 * @param msg the String that forms the message.
	 */
	public ECSMessage(TreeMap<String, String> metadata, StatusType status) {
		this.status = status;
		this.value = treeToString(metadata);
		this.msg = buildMsg();
		this.msgBytes = toByteArray(msg);
	}

	public ECSMessage(String msg) {
		this.msg = msg;
		setECS(msg);
		this.msgBytes = toByteArray(msg);
	}

	@Override
	public StatusType getStatus() {
		return status;
	}

	public String getValue() {
		return value.trim();
	}

	/**
	 * Returns the content of this ECSMessage as a String.
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

	public TreeMap<String, String> getValueAsMetadata() {
		TreeMap<String, String> metadata = new TreeMap<String, String>();
		String[] entries_arr = value.split(",");
		for (String entry_str: entries_arr) {
			String[] entry_split = entry_str.split("=");
			metadata.put(entry_split[0], entry_split[1]);
		}
		
		return metadata;
	}

	private void setECS(String msg) {
		String[] splitted = msg.split("~");
		this.value = splitted[0].trim();
		this.status = StatusType.valueOf(splitted[1].trim());
	}

	private String treeToString(TreeMap<String, String> tree) {
		StringBuilder treeAsString = new StringBuilder();
		for (String hash : tree.keySet()) {
			treeAsString.append(hash + "=" + tree.get(hash) + ",");
		}
		// delete , at end
		treeAsString.deleteCharAt(treeAsString.length() -1);

		return treeAsString.toString();
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
		return value.trim() + "~" + status.name();
	}
}
