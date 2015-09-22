package com.boxymoron.ha;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.boxymoron.ha.ClusterMember.Listener;
import com.boxymoron.ha.ClusterMember.State;


public class ClusterMemberTest {
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
		ClusterMember.joinCluster(new Listener(){
			@Override
			public void onStateChange(State state) {
				System.out.println("State: "+state);
			}
			
		});
		Thread.sleep(999999999);
		
	}
	
}