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
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements a simple HA Master/Slave clustering mechanism through what is essentially a state machine.<br><br>
 * The valid states are: MASTER, SLAVE, UNDEFINED<br><br>
 * <li>Each node in the cluster maintains a static list of all cluster member IP addresses/hostnames.<br>
 * <li>Each node sends a message with its own 'priority', which is a an integer in the range 0 - 9999 every {@link ClusterMember#timeout_ms}/2 ms.<br>
 * <li>Each node listens to all other node's messages.<br>
 * <li>The node with the highest priority is the MASTER.<br><br>
 * For this mechanism to work correctly, each node should have a different priority.<br><br>
 * 
 * The API provides a joinCluster({@link ClusterMember.Listener}) method to register an state changed listener. The listener can then
 * be used to control application specific behavior, such as starting/stopping services.
 * 
 * @author lnra54@gmail.com
 *
 */
public class ClusterMember {

	private static Properties props = new Properties();

	/**
	 * Sets the UDP datagram's payload width (fixed).
	 * This sets a max value of 9999 for property 'priority'
	 */
	private static final int BUFF_SIZE = 4;
	
	/**
	 * The number of milliseconds to wait for a ping from *any* other member in the cluster.
	 */
	private static int timeout_ms = 31000;
	private static int port = 8888;
	private static List<InetAddress> members = new ArrayList<InetAddress>();

	private static int priority;

	private static volatile boolean isInitialized = false;

	private static AtomicInteger timeoutCount = new AtomicInteger();
	private static AtomicInteger totalTimeoutCount = new AtomicInteger(); 

	private static CountDownLatch latch = new CountDownLatch(1);

	/**
	 * Valid states.
	 *
	 */
	public static enum State {
		MASTER, SLAVE, UNDEFINED
	}

	/**
	 * A listener's onStateChange method is invoked whenever the cluster's state changes.
	 *
	 */
	public static interface Listener {
		public void onStateChange(State state);
	}

	private static volatile State state = State.UNDEFINED;

	/**
	 * This method blocks until an initial state is determined. This can take up to {@link ClusterMember#timeout_ms} milliseconds
	 * @param listener
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
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

		startStatusCheckerThread(caller, listener);

		startPingThread(caller, listener);

		isInitialized = true;
		latch.await();
	}

	private static void startPingThread(final Thread caller, final Listener listener) {
		final Thread pingThread = new Thread(new Runnable(){
			@Override
			public void run() {
				DatagramSocket socket = null;
				try{
					socket = new DatagramSocket();
					byte[] buff = String.format("%"+BUFF_SIZE+"d", priority).getBytes("ASCII");//pad with zeroes on left

					while(true){
						for(InetAddress member : members){
							final DatagramPacket packet = new DatagramPacket(buff, 0, buff.length, member, port);
							System.out.println("Sending priority: "+priority+" to: "+member);
							socket.send(packet);
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

		pingThread.setName("pingThread");
		pingThread.setDaemon(true);
		pingThread.start();
	}

	private static Pattern integer = Pattern.compile("\\s*(\\d+)\\s*");

	private static void startStatusCheckerThread(final Thread caller, final Listener listener) {
		final Thread keepAliveThread = new Thread(new Runnable(){
			@Override
			public void run() {
				try(final DatagramSocket sock = new DatagramSocket(port);){
					sock.setSoTimeout(timeout_ms);
					sock.setTrafficClass(4);
					byte[] buff = new byte[BUFF_SIZE];
					int count = 0;
					final DatagramPacket packet = new DatagramPacket(buff, BUFF_SIZE);
					while(true){
						try{
							sock.receive(packet);
							
							final String dataStr = new String(packet.getData(), "ASCII");
							System.out.println("Received packet: "+dataStr+" from: "+packet.getAddress());
							final Matcher matcher = integer.matcher(dataStr);
							if(!matcher.find()){
								throw new NumberFormatException("invalid priority(int) received: "+dataStr);
							}
							final int otherPriority = Integer.parseInt(matcher.group(1));
							if(otherPriority < priority && !State.MASTER.equals(state)){
								state = State.MASTER;
								listener.onStateChange(state);
							}else if(otherPriority > priority && !State.SLAVE.equals(state)){
								state = State.SLAVE;
								listener.onStateChange(state);
							}else if(otherPriority == priority && (!State.UNDEFINED.equals(state) || count == 0)){//handle initial UNDEFINED state
								state = State.UNDEFINED;
								listener.onStateChange(state);
							}
							count++;
						}catch(NumberFormatException nfe){
							nfe.printStackTrace();
							state = State.UNDEFINED;
							listener.onStateChange(state);
						}catch(SocketTimeoutException ste){
							//ste.printStackTrace();
							System.out.println("Packet timed out.");
							totalTimeoutCount.incrementAndGet();
							timeoutCount.incrementAndGet();
							if(!State.MASTER.equals(state)){
								state = State.MASTER;
								listener.onStateChange(state);
							}
						}catch(SocketException se2){
							se2.printStackTrace();
							state = State.UNDEFINED;
							listener.onStateChange(state);
							caller.interrupt();
						}catch(IOException ioe){
							ioe.printStackTrace();
							if(sock.isClosed()){
								throw new RuntimeException("Socket is closed.");
							}
						}
					}
				}catch(SocketException se){
					se.printStackTrace();
				}
			}
		});

		keepAliveThread.setDaemon(true);
		keepAliveThread.setName("statusChecker");
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

		final Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
		final String[] addrs = props.getProperty("members").split(",");
		for(String addr : addrs){
			final InetAddress currInetAddr = InetAddress.getByName(addr);
			members.add(currInetAddr);
		}
	}

	public static void loadProperties(Properties properties, String defaultPropsLocation) throws IOException, FileNotFoundException {
		final String propertiesFile = System.getProperty(defaultPropsLocation);
		if(null == propertiesFile){
			final InputStream is = ClusterMember.class.getClassLoader().getResourceAsStream(defaultPropsLocation);
			try{
				properties.load(is);
			}finally{
				is.close();
			}
		}else if(new File(propertiesFile).canRead()){
			final Reader reader = new FileReader(propertiesFile);
			try{
				properties.load(reader);
			}finally{
				reader.close();
			}
		}else{
			throw new IOException(new StringBuilder("Invalid ").append(defaultPropsLocation).append(" file: ").append(propertiesFile).toString());
		}
	}

}