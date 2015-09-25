package com.boxymoron.ha;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import net.tomp2p.connection.NATUtils;
import net.tomp2p.natpmp.NatPmpException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.boxymoron.ha.ClusterMember.Listener;
import com.boxymoron.ha.ClusterMember.State;


public class ClusterMemberTest {
	
	private static Logger logger = LoggerFactory.getLogger(ClusterMemberTest.class);
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException, NatPmpException {
		logger.info("Starting");
		
		final Properties props = new Properties();
		ClusterMember.loadProperties(props, "cluster.properties");
		if(Boolean.parseBoolean(props.getProperty("enablePortForwarding", "false"))){
			setPortForwarding(Integer.parseInt(props.getProperty("port")));
		}

		ClusterMember.joinCluster(new Listener(){
			@Override
			public void onStateChange(State state) {
				logger.info("State changed to: "+state);
			}
			
		});
		while(true){
			logger.info("State: "+ClusterMember.state);
			try{
			Thread.sleep(21000);
			}catch(InterruptedException ie){
				Thread.interrupted();
				ie.printStackTrace();
			}
		}
		
	}
	private static void setPortForwarding(int port) throws IOException, FileNotFoundException, NatPmpException {
		final NATUtils natUtils = new NATUtils();
		logger.info("Setting port mapping for port: "+port);
		try{
			natUtils.mapUPNP("localhost", port, port, port, port);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			natUtils.unmapUPNP();
			natUtils.mapPMP(port, port, port, port);
		}
	}
	
}