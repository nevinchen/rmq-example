package com.stcoder.rmq.basic;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.stcoder.rmq.BaseRMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MessageProducer extends BaseRMQ {

    private static Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    public static void main(String[] args) throws Exception {
        String exchangeName = "directExchange";
        String queueName = "directQueue";
        String routingKey = "myKey";

        Connection connection = createConnection(rmqHost, rmqPort, username, password, virtualHost);
        Channel channel = connection.createChannel();
        AMQP.Exchange.DeclareOk exchangeDeclareResult = channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
        logger.info("Exchange {} is declared successfully with protocol method name {}."
                , exchangeName, exchangeDeclareResult.protocolMethodName());

        AMQP.Queue.DeclareOk queueDeclareResult = channel.queueDeclare(queueName, true, false, false, null);
        logger.info("Queue {} is declared successfully with consumer count {} and message count {}."
                , queueDeclareResult.getQueue(), queueDeclareResult.getConsumerCount(), queueDeclareResult.getMessageCount());

        String message = "Hello World!";

        Map<String, Object> headers = new HashMap<>();
        headers.put("color", "blue");
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().headers(headers).build();

        channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
        logger.info("Publish message successfully");

        channel.close();
        connection.close();
    }

}
