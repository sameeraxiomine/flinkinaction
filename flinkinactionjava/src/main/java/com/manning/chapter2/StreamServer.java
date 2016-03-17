package com.manning.chapter2;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.flink.shaded.com.google.common.base.Throwables;

public class StreamServer {
    private final int port;
    private final String[] sourcedata;
    private final int pauseEveryIthIndex;
    private int sleepIntervalInMillis = 1000;
    public StreamServer(int port,String[] sourcedata, int pauseEveryIthIndex,
            int sleepIntervalInMillis) {
        this.port = port;
        this.sourcedata = sourcedata;
        this.pauseEveryIthIndex = pauseEveryIthIndex;
        this.sleepIntervalInMillis = sleepIntervalInMillis;
    }
    public void startServer() {

        Runnable serverTask = () -> {
						try{
								ServerSocket serverSocket = new ServerSocket(port);
								//Start simulation after 5 seconds
								Thread.sleep(100);
								Socket socket =  null;
								try {
										socket = serverSocket.accept();
										for (int i = 0; i < sourcedata.length; i++) {
												PrintWriter out = new PrintWriter(
																socket.getOutputStream(), true);
												out.println(sourcedata[i]);
												if ((i + 1) % pauseEveryIthIndex == 0) {
														Thread.sleep(sleepIntervalInMillis);
												}
										}
								} finally {
										if(socket!=null) socket.close();
										serverSocket.close();
								}

						}catch(Exception ex){
								Throwables.propagate(ex);
						}

				};
        Thread serverThread = new Thread(serverTask);
        serverThread.start();

    }
}