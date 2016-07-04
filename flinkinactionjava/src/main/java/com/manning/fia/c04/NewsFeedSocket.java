package com.manning.fia.c04;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by hari on 6/25/16.
 */
public class NewsFeedSocket extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(NewsFeedSocket.class);
    private static final String DEFAULT_FILE_NAME = "/media/pipe/newsfeed";
    private static final int SLEEP_INTERVAL = 0;
    private static int PORT_NUMBER = 9000;

    private final String fileName;

    private final int threadSleepInterval;

    private final int portNumber;
    public static boolean stop = false;

    NewsFeedSocket() {
        this.fileName = DEFAULT_FILE_NAME;
        this.threadSleepInterval = SLEEP_INTERVAL;
        this.portNumber = PORT_NUMBER;
    }

    NewsFeedSocket(String fileName){
        this.fileName = fileName;
        this.threadSleepInterval = SLEEP_INTERVAL;
        this.portNumber = PORT_NUMBER;
    }
    NewsFeedSocket(String fileName, int threadSleepInterval, int portNumber) {
        this.fileName = fileName;
        this.threadSleepInterval = threadSleepInterval;
        this.portNumber = portNumber;
    }
    NewsFeedSocket(String fileName, int portNumber) {
        this.fileName = fileName;
        this.portNumber = portNumber;
        this.threadSleepInterval = 0;
    }
    @Override
    public void run() {
        try {
            write();
        } catch (Exception e) {
            LOG.error("Exception in NewsFeedSocket", e);
        }
    }


    public void write() throws Exception {

        final ServerSocket serverSocket = new ServerSocket(portNumber);
        final Socket socket = serverSocket.accept();
        final Scanner scanner = new Scanner(ClassLoader.class.getResourceAsStream(fileName));
        while (scanner.hasNext()) {
            final String value = scanner.nextLine() + '\n';
            IOUtils.write(value.getBytes(), socket.getOutputStream());
            Thread.currentThread().sleep(threadSleepInterval);
        }
        serverSocket.close();
    }


}

