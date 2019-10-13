package com.stcoder.rmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class BaseRMQ {

    protected static String rmqHost = "127.0.0.1";
    protected static int rmqPort = 5672;
    protected static String username = "username";
    protected static String password = "password";
    protected static String virtualHost = "host_1";

    protected static Connection createConnection(String host, int port, String username, String password,
                                             String virtualHost)
            throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost(virtualHost);
        return connectionFactory.newConnection();
    }

}
