package com.boxymoron.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a simple Partition-Tolerant Leader election algorithm (over UDP). The algorithm is essentially a state machine:<br><br>
 * The valid states are: MASTER, SLAVE, UNDEFINED<br><br>
 * <li>Each node in the cluster maintains a static list of all (other) cluster member IP addresses/hostnames.<br>
 * <li>Each node asynchronously sends a message with its own 'priority' (UDP), which is a an integer in the range 0 - 9999 every {@link ClusterMember#timeout_ms}/2 ms.<br>
 * <li>Each node listens to all other node's messages.<br>
 * <li>The node with the highest priority, in a majority partition, becomes the MASTER.<br><br>
 * For this mechanism to work correctly, each node should have a distinct priority. Currently, only odd numbers of nodes >= 3 are supported ;)<br><br>
 * 
 * The API provides a joinCluster({@link ClusterMember.Listener}) method to register a state changed listener. The listener can then
 * be used to control application specific behavior, such as starting/stopping services, replication, etc.
 * 
 * 
 * @author Leonardo Rodriguez-Velez
 *
 */
public class ClusterMember {
	
	private static Logger logger = LoggerFactory.getLogger(ClusterMember.class);

	private static Properties props = new Properties();

	/**
	 * Sets the UDP datagram's payload width (fixed).
	 * This sets a max value of 9999 for property 'priority'
	 */
	private static final int BUFF_SIZE = 4;
	
	/**
	 * The number of milliseconds to wait for a ping from *any* other member in the cluster.
	 */
	private static int timeout_ms = 30000;
	
	/**
	 * The port for UDP. This should be the same on all nodes.
	 */
	private static int port = 8888;
	
	/**
	 * A list of other nodes in this cluster.
	 */
	private static List<Member> members = new ArrayList<Member>();

	/**
	 * The priority of this node, between 0 and 9999 (inclusive)
	 */
	private static int priority;

	private static volatile boolean isInitialized = false;

	private static AtomicInteger timeoutCount = new AtomicInteger();
	private static AtomicInteger totalTimeoutCount = new AtomicInteger(); 

	/**
//	 * This is used to block the calling thread(main?) until an initial state is selected.
	 */
	private static CountDownLatch latch = new CountDownLatch(1);
	
	private static Thread statusThread = null;
	
	/**
	 * The initial state of each node is always 'UNDEFINED'.
	 */
	public static volatile State state = State.UNDEFINED;

	/**
	 * Valid states.
	 *
	 */
	public static enum State {
		MASTER, SLAVE, UNDEFINED
	}
	
	public static class Member {
		public InetAddress address;
		public volatile long lastRxPacket = System.currentTimeMillis();
		public volatile int priority = -1;
		public Member(InetAddress address, long lastRxPacket) {
			this.address = address;
			this.lastRxPacket = lastRxPacket;
		}
		public InetAddress getAddress() {
			return this.address;
		}
		public long getLastRxPacket() {
			return lastRxPacket;
		}
		public void setLastRxPacket(long lastRxPacket) {
			this.lastRxPacket = lastRxPacket;
		}
		public int getPriority() {
			return priority;
		}
		public void setPriority(int priority) {
			this.priority = priority;
		}
		public void setAddress(InetAddress address) {
			this.address = address;
		}
		public boolean isExpired(long currTS){
			return (currTS - lastRxPacket) > timeout_ms;
		}

		@Override
		public String toString() {
			return "Member [address=" + address + ", lastRxPacket="
					+ lastRxPacket + ", priority=" + priority + "]";
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((address == null) ? 0 : address.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Member other = (Member) obj;
			if (address == null) {
				if (other.address != null)
					return false;
			} else if (!address.equals(other.address))
				return false;
			return true;
		}
	}

	/**
	 * A listener's onStateChange method is invoked whenever the cluster's state changes.
	 *
	 */
	public static interface Listener {
		public void onStateChange(State state);
	}

	/**
	 * This method blocks until an initial state is determined. This can take up to {@link ClusterMember#timeout_ms} * 2 milliseconds. 
	 * This method should only be called once during the lifetime of this process!
	 * @param listener
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws IllegalStateException if called more than once.
	 */
	public static synchronized void joinCluster(final Listener listener) throws FileNotFoundException, IOException, InterruptedException {
		if(isInitialized){
			throw new IllegalStateException("Cannot join cluster more than once.");
		}

		final Thread caller = Thread.currentThread();

		initProps(new Listener(){

			@Override
			public void onStateChange(State state) {
				if(State.UNDEFINED.equals(state)){
					throw new IllegalStateException("Cluster state is UNDEFINED.");
				}
				latch.countDown();
				listener.onStateChange(state);
			}

		});

		startPriorityTXThread(caller, listener);
		startPriorityRXThread(caller, listener);
		
		isInitialized = true;
		latch.await(timeout_ms * 2, TimeUnit.MILLISECONDS);
		statusThread = startStatusThread(listener);
	}

	private static Thread startStatusThread(final Listener listener) {
		Thread statusThread = new Thread(new Runnable(){

			@Override
			public void run() {
				AtomicLong ts = new AtomicLong(System.currentTimeMillis());
				while(true){
					out:{
						if(System.currentTimeMillis() - ts.get() < timeout_ms){
							break out;
						}
						ts.set(System.currentTimeMillis());
						if(!State.UNDEFINED.equals(state) && members.stream().anyMatch(m -> m.priority == priority)){
							state = State.UNDEFINED;
							listener.onStateChange(state);
							logger.info("State changed to: "+state+" "+4);
						}else {
							if(!State.SLAVE.equals(state) && members.stream().anyMatch(m -> !m.isExpired(ts.get()) && m.priority > priority)){
								state = State.SLAVE;
								listener.onStateChange(state);
								logger.info("State changed to: "+state+" "+3);
								break out;
							}
							if(!State.MASTER.equals(state)){
								final List<Member> membersCurr = members.stream().filter(m -> !m.isExpired(ts.get())).collect(Collectors.toList());
								//we add one since the members doesn't include *this* node
								if((membersCurr.size() + 1) >= ((int)((members.size()+1) / 2) + 1) && membersCurr.stream().allMatch(m -> m.priority < priority)){
									state = State.MASTER;
									listener.onStateChange(state);
									logger.info("State changed to: "+state+" "+5);
									break out;
								}
							} 
							if(!State.UNDEFINED.equals(state) && members.stream().filter(m -> !m.isExpired(ts.get())).count() == 0){//I'm running all by myself
									state = State.UNDEFINED;
									listener.onStateChange(state);
									logger.info("State changed to: "+state+" "+7);
							}else if(State.MASTER.equals(state)){
								final List<Member> membersCurr = members.stream().filter(m -> !m.isExpired(ts.get())).collect(Collectors.toList());
								if(membersCurr.size() > 0 && membersCurr.stream().anyMatch(m -> m.priority > priority)){
									state = State.SLAVE;
									listener.onStateChange(state);
									logger.info("State changed to: "+state+" "+6);
								}else if(membersCurr.size() + 1 < ((int)((members.size()+1) / 2) + 1)){//Network Partition?
									state = State.UNDEFINED;
									listener.onStateChange(state);
									logger.info("State changed to: "+state+" "+8);
								}
							}else{
								logger.info("State: "+state+" ");
							}
						}
					}
					try {
						if(!Thread.interrupted()){
							Thread.sleep(1000);
						}
					} catch (InterruptedException e) {
						Thread.interrupted();
					}
				}
			}
			
		});
		statusThread.setName("statusThrd");
		statusThread.setDaemon(true);
		statusThread.start();
		return statusThread;
	}

	private static void startPriorityTXThread(final Thread caller, final Listener listener) {
		final Thread pingThread = new Thread(new Runnable(){
			@Override
			public void run() {
				DatagramSocket socket = null;
				try{
					socket = new DatagramSocket();
					byte[] buff = String.format("%"+BUFF_SIZE+"d", priority).getBytes("ASCII");//pad with zeroes on left

					while(true){
						for(Member member : members){
							final DatagramPacket packet = new DatagramPacket(buff, 0, buff.length, member.getAddress(), port);
							logger.info("Sending priority: "+priority+" to: "+member);
							try{
								socket.send(packet);
							}catch(IOException ioe2){
								ioe2.printStackTrace();
								//the show must go on
								if(socket.isClosed()){
									socket.disconnect();
									socket = new DatagramSocket();
								}
							}
						}
						try{
							Thread.sleep(timeout_ms/2);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}catch(Exception ioe){
					ioe.printStackTrace();
					listener.onStateChange(State.UNDEFINED);
					caller.interrupt();
				}finally{
					if(socket != null){
						socket.close();
					}
				}
			}
		});

		pingThread.setName("priorityTX");
		pingThread.setDaemon(true);
		pingThread.start();
	}

	private static Pattern integer = Pattern.compile("\\s*(\\d+)\\s*");

	private static void startPriorityRXThread(final Thread caller, final Listener listener) {
		final Thread keepAliveThread = new Thread(new Runnable(){
			@Override
			public void run() {
				DatagramSocket sock = null;
				try{
					sock = new DatagramSocket(port);
					sock.setSoTimeout(timeout_ms);
					sock.setTrafficClass(4);
					byte[] buff = new byte[BUFF_SIZE];
					int count = 0;
					final DatagramPacket packet = new DatagramPacket(buff, BUFF_SIZE);
					while(true){
						try{
							long ts;
							start:{
								sock.receive(packet);
								ts = System.currentTimeMillis();
								
								final String dataStr = new String(packet.getData(), "ASCII");

								for(Member m : members){
									if(m.address.equals(packet.getAddress())){
										m.setLastRxPacket(ts);
										final Matcher matcher = integer.matcher(dataStr);
										if(!matcher.find()){
											throw new NumberFormatException("invalid priority(int) received: "+dataStr);
										}
										final int otherPriority = Integer.parseInt(matcher.group(1));
										m.setPriority(otherPriority);
										logger.info("Received packet: "+dataStr+" from: "+m);
										
										if(m.getPriority() == priority && (!State.UNDEFINED.equals(state) || count == 0)){//handle initial UNDEFINED state
											state = State.UNDEFINED;
											listener.onStateChange(state);
											break start;
										}
									}
								}
								if(statusThread != null){
									statusThread.interrupt();
								}
							}
							
							count++;
						}catch(NumberFormatException nfe){
							nfe.printStackTrace();
							state = State.UNDEFINED;
							listener.onStateChange(state);
							if(statusThread != null){
								statusThread.interrupt();
							}
						}catch(SocketTimeoutException ste){
							//ste.printStackTrace();
							logger.info("Packet timed out.");
							totalTimeoutCount.incrementAndGet();
							timeoutCount.incrementAndGet();
							if(statusThread != null){
								statusThread.interrupt();
							}
						}catch(SocketException se2){
							se2.printStackTrace();
							state = State.UNDEFINED;
							listener.onStateChange(state);
							caller.interrupt();
							if(statusThread != null){
								statusThread.interrupt();
							}
						}catch(IOException ioe){
							ioe.printStackTrace();
							if(statusThread != null){
								statusThread.interrupt();
							}
							if(sock.isClosed()){
								sock.disconnect();
								sock = new DatagramSocket(port);
								//the show must go on
								//throw new RuntimeException("Socket is closed.");
							}
						}
					}
				}catch(SocketException se){
					se.printStackTrace();
				}
			}
		});

		keepAliveThread.setDaemon(true);
		keepAliveThread.setName("priorityRX");
		keepAliveThread.start();
	}

	private static synchronized void initProps(Listener listener) throws IOException, FileNotFoundException {

		if(listener == null){
			throw new IllegalArgumentException("Listener cannot be null");
		}

		loadProperties(props, "cluster.properties");

		if(null == props.getProperty("priority")){
			throw new RuntimeException("cluster.properties is missing boolean property 'priority'");
		}

		if(null == props.getProperty("members")){
			throw new RuntimeException("cluster.properties is missing boolean property 'members'");
		}

		priority = Integer.parseInt(props.getProperty("priority"));
		timeout_ms = Integer.parseInt(props.getProperty("timeout_ms", ""+timeout_ms));

		if(timeout_ms < 100 || timeout_ms > 99999){
			throw new IllegalStateException("Invalid property value: timeout_ms="+timeout_ms);
		}

		port = Integer.parseInt(props.getProperty("port", ""+port));

		final String[] addrs = props.getProperty("members").split(",");
		for(String addr : addrs){
			final InetAddress currInetAddr = InetAddress.getByName(addr);
			members.add(new Member(currInetAddr, -1));
			logger.info("Registering remote cluster member: "+currInetAddr);
		}
	}

	public static void loadProperties(Properties properties, String defaultPropsLocation) throws IOException, FileNotFoundException {
		final String propertiesFile = System.getProperty(defaultPropsLocation);
		if(propertiesFile != null && new File(propertiesFile).canRead()){
			final Reader reader = new FileReader(propertiesFile);
			try{
				properties.load(reader);
			}finally{
				reader.close();
			}
		}else if(null == propertiesFile){
			final InputStream is = ClusterMember.class.getClassLoader().getResourceAsStream(defaultPropsLocation);
			try{
				properties.load(is);
			}finally{
				is.close();
			}
		}else{
			throw new IOException(new StringBuilder("Invalid ").append(defaultPropsLocation).append(" file: ").append(propertiesFile).toString());
		}
		for(Entry<Object, Object> entry : properties.entrySet()){
			logger.info(entry.getKey()+"="+entry.getValue());
		}
	}

}