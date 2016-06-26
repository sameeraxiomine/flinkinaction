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

    @Override
    public void run() {
        try {
            write();
        } catch (Exception e) {
            LOG.error("Exception in NewsFeedSocket", e);
        }
    }

    private static int PORT_NUMBER = 9000;

    public void write() throws Exception {

        final ServerSocket serverSocket = new ServerSocket(PORT_NUMBER);
        final Socket socket = serverSocket.accept();
        final Scanner scanner = new Scanner(ClassLoader.class.getResourceAsStream("/media/pipe/newsfeed"));
        while (scanner.hasNext()) {
            final String value = scanner.nextLine() + '\n';
            IOUtils.write(value.getBytes(), socket.getOutputStream());
        }

    }


}

