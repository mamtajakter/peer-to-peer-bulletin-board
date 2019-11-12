
import java.io.Serializable;

class PacketFormat implements Serializable {

	private static final long serialVersionUID = 1L;

	private int messageTypeId;
	private int sourceId;
	private int srcSpecMsgId_ElectionId_TokenId;
	private String userPost;
	private int bestCandId_electedClientId;

	public int getMessageTypeId() {
		return messageTypeId;
	}

	public int getSourceId() {
		return sourceId;
	}

	public int getSrcSpecMsgId_ElectionId_TokenId() {
		return srcSpecMsgId_ElectionId_TokenId;
	}

	public String getUserPost() {
		return userPost;
	}

	public int getBestCandId_electedClientId() {
		return bestCandId_electedClientId;
	}

	// Post(0)
	PacketFormat(int messageTypeId, int sourceId, int srcSpecMsgId_ElectionId_TokenId, String userPost) {
		this.messageTypeId = messageTypeId;
		this.sourceId = sourceId;
		this.srcSpecMsgId_ElectionId_TokenId = srcSpecMsgId_ElectionId_TokenId;
		this.userPost = userPost;
	}

	// Election(20) Elected(21)
	PacketFormat(int messageTypeId, int sourceId, int srcSpecMsgId_ElectionId_TokenId, int bestCandId_electedClientId) {
		this.messageTypeId = messageTypeId;
		this.sourceId = sourceId;
		this.srcSpecMsgId_ElectionId_TokenId = srcSpecMsgId_ElectionId_TokenId;
		this.bestCandId_electedClientId = bestCandId_electedClientId;
	}

	// Probe(10) ProbeAck(11) ProbeNack(12) Token(30)
	PacketFormat(int messageTypeId, int sourceId, int srcSpecMsgId_ElectionId_TokenId) {
		this.messageTypeId = messageTypeId;
		this.sourceId = sourceId;
		this.srcSpecMsgId_ElectionId_TokenId = srcSpecMsgId_ElectionId_TokenId;
	}
}