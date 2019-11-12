
import java.io.Serializable;

import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Scanner;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TimeZone;

import java.util.Set;

public class Node {

	private static final int PROBE = 10;
	private static final int PROBE_ACK = 11;
	private static final int PROBE_NACK = 11;
	private static final int ELECTION = 20;
	private static final int ELECTED = 21;
	private static final int TOKEN = 30;
	private static long TIMEOUT = 5000;
	private static final long SLEEP = 1000;
	private static final int POST = 0;
	public static String OUTPUT = "";
	private long startTime=0;
	private long endTime=0;
	private long tat=0;

	private DatagramSocket socket;
	private int myPort;
	private boolean IsRingFormed;
	private int lowestPort, highestPort;
	private int nextHop, previousHop;
	private long goLiveTime;
	private long join, leave;
	private Receive receiveThread;
	private boolean alive;
	private boolean probeSuccess;
	private Probe probingThread;
	private long lastMessageTime;

	private int electionId;
	private int tokenId;
	private long tokenstamp;
	private boolean electionStarted;
	private boolean elected;
	private Set<Integer> knownTokens;

	private NavigableMap<Long, String> messages;

	Node(int port, int rangeStart, int rangeEnd, long join, long leave) {// , long join, long leave) {
		this.myPort = port;
		this.lowestPort = rangeStart;
		this.highestPort = rangeEnd;
		this.join = join;
		this.leave = leave;
		this.nextHop = -1;
		this.previousHop = -1;
		this.tokenId=-1;
		this.electionId=-1;
		this.tokenstamp=-1;
		this.elected=false;
		this.electionStarted=false;
		this.setAlive(true);
		this.probeSuccess = false;
		this.knownTokens = new HashSet<Integer>();
	}

	private static SimpleDateFormat formatter = new SimpleDateFormat("mm:ss");
	static {
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public boolean isAlive() {
		return alive;
	}

	public void reInitPorts() {
		this.nextHop = -1;
		this.previousHop = -1;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	private void setMessages(NavigableMap<Long, String> messages) {
		this.messages = messages;
	}

	public void setElectionStarted(boolean electionStarted) {
		this.electionStarted=true;
	}
	public void resetElectionStarted(boolean electionStarted) {
		this.electionStarted=false;
	}

	public void setElected(boolean elected) {
		this.elected=elected;
	}
	public boolean getElectionStarted() {
		return electionStarted;
	}
	public boolean getElected() {
		return elected;
	}


	public long getJoin() {
		return join;
	}

	private String getTimestamp() {
		return "" + formatter.format(new Date(System.currentTimeMillis() - goLiveTime)) + "";
	}

	public long getLeave() {
		return leave;
	}

	public static void setOutputFile(String fileName) throws IOException, ParseException {

		FileWriter pw = new FileWriter(fileName);

		try {
			pw.write(OUTPUT);
		} catch (IOException e) {

		}
		pw.close();

	}

	public void generateElectionId() {
		this.electionId = new Random().nextInt(100000);


		this.tokenstamp = System.currentTimeMillis() - goLiveTime;
	}

	private void updateToken(PacketFormat pf) {
		this.tokenId = pf.getSrcSpecMsgId_ElectionId_TokenId();
		this.tokenstamp = System.currentTimeMillis() - goLiveTime;
	}

	public void send(PacketFormat p, int destPort) throws PeerException {
		if (isAlive() == false)
			return;

		byte buffer[] = convertMessageToBytes(p);
		InetAddress address = null;
		try {
			address = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new PeerException("Failed to get address for the serving peer : " + e.getMessage());
		}
		DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, destPort);
		try {
			socket.send(packet);
			//socket.setSoTimeout((int)TIMEOUT);
		} catch (IOException e) {
			throw new PeerException("Failed to send request to the serving peer" + e.getMessage());
		}
	};

	class Probe extends Thread {

		public Probe() {
			reInitPorts();
			//elected = false;
			setElected(false);
//			resetElectionStarted(false);
		}

		@Override
		public void run() {
			probeSuccess = false;
			byte buffer[] = new byte[256];
			PacketFormat pf = null;
			int counter=0;
			for (int probePort = myPort + 1; probePort <= highestPort + 1; probePort++) {

				if (probePort == highestPort + 1)
					probePort = lowestPort;

				try {

					int srcSpecMsgId = counter;
					pf = new PacketFormat(PROBE, myPort, srcSpecMsgId);
					send(pf, probePort);
					Thread.sleep(SLEEP);
				} catch (PeerException | InterruptedException e) {
					System.err.println("Error occured while sending probing message");
					System.exit(1);
				}
				if (probeSuccess) {
					nextHop = probePort;
					break;
				}
				counter++;
			}
			//System.out.println(probeSuccess);
			if (probeSuccess) {
				try {
					sleep(new Random().nextInt(1000));
				} catch (InterruptedException e) {
					System.err.println("Error occurred while initializing election");
					System.exit(1);
				}
				try {
					//if (getElectionStarted()==false) {
					generateElectionId();
					pf = new PacketFormat(ELECTION, myPort, electionId, myPort);
					send(pf, nextHop);
					String out = "";
					out = getTimestamp() + ": started election, send election message to client " + nextHop + "\n";
					OUTPUT += out;
					//System.out.println(myPort + ": " + out+pf.getSrcSpecMsgId_ElectionId_TokenId());
//					setElectionStarted(true);
//					setElected(false);
					//}
				} catch (PeerException e) {
					System.err.println("Error occurred while sending election");
					System.exit(1);
				}
			}
			/*PROBE_NACK
			else {
				try {
					//generateElectionId();
					pf = new PacketFormat(PROBE_NACK, myPort, 1);
					send(pf, nextHop);
					String out = "";
					out = getTimestamp() + ": started election, send election message to client [" + nextHop + "]\n";
					OUTPUT += out;

				} catch (PeerException e) {
					System.err.println("Error occurred while sending election");
					System.exit(1);
				}
			}
			PROBE_NACK*/

		}
	};

	@SuppressWarnings("unused")
	public void sendMessages() throws PeerException {
		PacketFormat pf = null;

		NavigableMap<Long, String> headMap = messages.headMap(tokenstamp, true);

		if (headMap.isEmpty()) {

			pf = new PacketFormat(TOKEN, myPort, tokenId);
			send(pf, nextHop);
			if (!knownTokens.contains(tokenId)) {

				String out = "";
				out = getTimestamp() + ": token [" + tokenId + "] was sent to client " + nextHop + "\n";
				OUTPUT += out;
				//System.out.println(myPort + ": " + out);

				knownTokens.add(tokenId);
			}
			this.tokenId = -1;
			this.tokenstamp = -1;
//			setElectionStarted(false);
//			setElected(false);
		} else {
			Long messagetime = headMap.firstKey();
			int msgTypeId = 0;

			String toPost = headMap.get(messagetime);
			String[] result = toPost.split(":");
			int srcPecSeqNo = Integer.parseInt(result[0]);
			String msg = result[1];

			pf = new PacketFormat(POST, myPort, srcPecSeqNo, messagetime + ":" + msg);
			send(pf, nextHop);

			String out = "";
			out = getTimestamp() + ": post \"" + msg + "\" was sent\n";
			OUTPUT += out;
			//System.out.println(myPort + ": " + out);

			startTime=System.currentTimeMillis();

		}
	}

	public static byte[] convertMessageToBytes(PacketFormat msg) {
		byte[] buffer = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(msg);
			out.flush();
			buffer = bos.toByteArray();
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				bos.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return buffer;
	}

	public static PacketFormat convertBytesToMessage(byte[] buffer) {
		PacketFormat msg = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;
		try {
			bis = new ByteArrayInputStream(buffer);
			ois = new ObjectInputStream(bis);
			msg = (PacketFormat) ois.readObject();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (bis != null) {
				try {
					bis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (ois != null) {
				try {
					ois.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return msg;
	}

	class Receive extends Thread {
		@Override
		public void run() {
			String out = "";

			while (Node.this.isAlive()) {
				try {
					byte buffer[] = new byte[256];
					DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
					socket.receive(packet);
					//socket.setSoTimeout((int)TIMEOUT);

					int senderPort = packet.getPort();

					PacketFormat pf = convertBytesToMessage(buffer);
					if (pf.getMessageTypeId() == PROBE) {
						if (previousHop == -1 || (Math.floorMod(senderPort - myPort, highestPort - lowestPort) > Math
								.floorMod(previousHop - myPort, highestPort - lowestPort))) {
							previousHop = senderPort;
							out = getTimestamp() + ": previous hop is changed to client " + previousHop + "\n";
							OUTPUT += out;
							//System.out.println(myPort + ": " + out);


							PacketFormat pf1 = new PacketFormat(PROBE_ACK, pf.getSourceId(),
									pf.getSrcSpecMsgId_ElectionId_TokenId());
							send(pf1, senderPort);
							lastMessageTime=System.currentTimeMillis();
						}
					} else if (pf.getMessageTypeId() == PROBE_ACK) {
						if (nextHop == -1 || (Math.floorMod(senderPort - myPort, highestPort - lowestPort) < Math
								.floorMod(nextHop - myPort, highestPort - lowestPort))) {
							nextHop = senderPort;
							probeSuccess = true;
							out = getTimestamp() + ": next hop is changed to client " + nextHop + "\n";
							OUTPUT += out;
							//System.out.println(myPort + ": " + out+pf.getSourceId());
							lastMessageTime=System.currentTimeMillis();
						}
					}

					if (!probeSuccess || senderPort != previousHop)
						continue;

					if (pf.getMessageTypeId() == ELECTION && !getElected()) {
						int electionSourcePort = pf.getSourceId();

						if (electionSourcePort < myPort) {
							PacketFormat pf1 = new PacketFormat(ELECTION, pf.getSourceId(), pf.getSrcSpecMsgId_ElectionId_TokenId(), myPort);
							send(pf1, nextHop);
							out = getTimestamp() + ": relayed election message, replaced leader\n";
							OUTPUT += out;
							//System.out.println(myPort + ": " + out+ pf.getSrcSpecMsgId_ElectionId_TokenId());
//							setElectionStarted(true);
//							setElected(false);

						} else if (electionSourcePort > myPort) {

							send(pf, nextHop);

							out = getTimestamp() + ": relayed election message, leader: client " + pf.getBestCandId_electedClientId()
									+ "\n";
							OUTPUT += out;
							//System.out.println(myPort + ": " + out+ pf.getSrcSpecMsgId_ElectionId_TokenId());
//							setElectionStarted(true);
//							setElected(false);

						} else {

							PacketFormat pf1;
							pf1 = new PacketFormat(ELECTED, pf.getSourceId(), pf.getSrcSpecMsgId_ElectionId_TokenId(), myPort);
							send(pf1, nextHop);
//							setElectionStarted(true);
							setElected(true);
						}
						lastMessageTime=System.currentTimeMillis();
					} else if (pf.getMessageTypeId() == ELECTED) {
						int electionSourcePort = pf.getBestCandId_electedClientId();

						if (electionSourcePort < myPort) {
							PacketFormat pf1 = new PacketFormat(ELECTED, pf.getSourceId(),
									pf.getSrcSpecMsgId_ElectionId_TokenId(), myPort);

							send(pf1, nextHop);
//							setElectionStarted(true);
							setElected(true);
						} else if (electionSourcePort > myPort) {
//							setElectionStarted(true);
							setElected(false);

							send(pf, nextHop);
						} else {

							out = getTimestamp() + ": leader selected\n";
							OUTPUT += out;
							//System.out.println(myPort + ": " + out);
							//electionId=-1;

							tokenId=pf.getSrcSpecMsgId_ElectionId_TokenId();
							//String out = "";
							//generateElectionId();

							out = getTimestamp() + ": new token generated [" + tokenId + "]\n";
							OUTPUT += out;
							//System.out.println(myPort + ": " + out);
//							setElectionStarted(false);
//							setElected(false);

							sendMessages();

						}
						lastMessageTime=System.currentTimeMillis();
					} else if (pf.getMessageTypeId() == TOKEN) {
						updateToken(pf);// tokenId=pf.getElectionID();
						if (!knownTokens.contains(tokenId)) {
							out = getTimestamp() + ": token [" + tokenId + "] was received\n";
							OUTPUT += out;
							//System.out.println(myPort + ": " + out);

						}
						sendMessages();
						lastMessageTime=System.currentTimeMillis();
					}

					else if (pf.getMessageTypeId() == POST) {
						String[] split = pf.getUserPost().split(":");
						int messageOrigin = pf.getSourceId();
						long messagetime = Long.parseLong(split[0].trim());
						String toPost = split[1].trim();
						if (messageOrigin == myPort) {
							if (!messages.containsKey(messagetime)) {
								System.out.println("Message is not what was send out");
							} else {

								out = getTimestamp() + ": post \"" + toPost + "\" was delivered to all successfully\n";
								OUTPUT += out;
								//System.out.println(myPort + ": " + out);

								messages.remove(messagetime);

								endTime=System.currentTimeMillis();
								tat=endTime-startTime;
								if (tat==0)
									tat=100;
								TIMEOUT=tat*2;
								sendMessages();
							}
						} else {

							out = getTimestamp() + ": post \"" + toPost + "\" from client " + messageOrigin
									+ " was relayed\n";
							OUTPUT += out;
							//System.out.println(myPort + ": " + out);
							send(pf, nextHop);
						}
						lastMessageTime=System.currentTimeMillis();
					}


				} catch (SocketTimeoutException e) {
					out = getTimestamp() + ": ring is broken\n";
					OUTPUT += out;
					//System.out.println(myPort + ": " + out);
					probingThread = new Probe();
					probingThread.start();
				} catch (PeerException | IOException e) {
					System.err.println("There was a problem while " + "receiving server request :" + e.getMessage()
							+ " " + e.getClass().getName());
					socket.close();
					System.exit(1);
				}

			}
//			/*New*/
//			if (!probingThread.isAlive()) {
//				System.out.println("Hello");
//				out = getTimestamp() + ": ring is broken\n";
//				OUTPUT += out;
//				System.out.println(myPort + ": " + out);
//
//				probingThread = new Probe();
//				probingThread.start();
//			}
//			/*NEW*/

		}

	};

	public void probe() {
		probingThread = new Probe();
		probingThread.start();
	}

	public void receive() {
		receiveThread = new Receive();
		receiveThread.start();
	};

	@SuppressWarnings("deprecation")
	private static void createRingFormation(Node node) throws PeerException {
		long goLive = node.getGoLiveTime();
		long join = node.getJoin();
		long leave = node.getLeave();
		while (((System.currentTimeMillis()) - goLive) < join) {
			try {
				Thread.sleep(SLEEP);
			} catch (InterruptedException e) {
				System.err.println("Error1");
				System.exit(1);
			}

		}
		node.setAlive(true);
		node.initSocket();

		node.receive();

		node.probe();

		while (((System.currentTimeMillis()) - goLive) < leave) {
			try {
				Thread.sleep(SLEEP);
			} catch (InterruptedException e) {
				System.err.println("Error2");
				System.exit(1);
			}
		}

		if (((System.currentTimeMillis()) - goLive) >= leave)  {


//	        	String out="";
//	        	out = node.getTimestamp()+": ring is broken\n";
//				OUTPUT += out;
//				System.out.println(myPort + ": "+ out);

			node.probe();

		}
		node.setAlive(false);
		node.receiveThread.stop();
		node.probingThread.stop();
		node.socket.close();
	}

	private void initSocket() throws PeerException {
		try {
			socket = new DatagramSocket(this.myPort);
			socket.setSoTimeout((int) TIMEOUT);
		} catch (SocketException e) {
			throw new PeerException("Error11");
		}

	}

	public long getGoLiveTime() {
		return goLiveTime;
	}

	public void setGoLiveTime(long goLiveTime) {
		this.goLiveTime = goLiveTime;
	}

	private static NavigableMap<Long, String> getMessageMap(String fileName) throws IOException, ParseException {
		NavigableMap<Long, String> messages = new TreeMap<Long, String>();
		int line = 0, counter = 0;
		long timestamp = 0;
		String msg = "";
		Scanner scanner = new Scanner(new File(fileName));

		while (scanner.hasNextLine()) {
			String str2 = scanner.nextLine();
			String[] result2 = str2.split("\t");
			msg = result2[1];

			timestamp = formatter.parse(result2[0].trim()).getTime();

			messages.put(timestamp, line + ":" + msg);
			line++;
		}
		scanner.close();

		return messages;
	}

	public static Node createCurrentNode(String fileName) {
		int lowerPort = 0;
		int higherPort = 0;
		int myPort = 0;
		int destinationPort = 0;
		Node node = null;
		long join = 0, leave = 0;

		String portRange;
		try {

			Scanner scanner1 = new Scanner(new File(fileName));
			int line = 0;
			while (scanner1.hasNextLine()) {

				String str1 = scanner1.nextLine();
				String[] result = str1.split(": ");
				if (line == 0) {
					portRange = result[1];
					String[] portranges = portRange.split("-");
					lowerPort = Integer.parseInt(portranges[0]);

					higherPort = Integer.parseInt(portranges[1]);

				} else if (line == 1) {
					myPort = Integer.parseInt(result[1]);

					if ((myPort < lowerPort) || (myPort > higherPort)) {
						System.out.println("Port Number is out of bound");
						System.exit(0);
					}
				} else if (line == 2) {

					join = formatter.parse(result[1].trim()).getTime();

				} else if (line == 3) {

					leave = formatter.parse(result[1].trim()).getTime();

				}

				line++;
			}
			scanner1.close();

			node = new Node(myPort, lowerPort, higherPort, join, leave);

		} catch (IOException | ParseException e) {

		}
		return node;
	}

	public static void main(String[] args) throws IOException, ParseException {

		int i = 0;
		String portRange;
		int myPort = 0, destinationPort = 0, lowerPort = 0, higherPort = 0;
		InetAddress aHost = InetAddress.getLocalHost();
		String destinationIP = aHost.getHostAddress();
		int line = 0;

		Node node = null;
		NavigableMap<Long, String> messages = null;

		try {
			node = createCurrentNode(args[1]);

			messages = getMessageMap(args[3]);
		} catch (IOException | ParseException e) {

		}
		if (node == null || messages == null) {
			System.err.println("Error7");
			System.exit(1);

		}

		node.setGoLiveTime(System.currentTimeMillis());

		node.setMessages(messages);
		try {
			createRingFormation(node);
		} catch (PeerException e) {
			System.err.println("Error8");
			System.exit(1);
		}
		setOutputFile(args[5]);
	}
}






//CMD+SHFT+F to beautify
//open terminal
//cd /Users/mamtajakter/eclipse-workspace/p2pudp1/src
//make clean
//make build
//make test1