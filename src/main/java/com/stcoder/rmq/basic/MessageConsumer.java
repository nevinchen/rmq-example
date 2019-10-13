package com.stcoder.rmq.basic;

import com.rabbitmq.client.*;
import com.stcoder.rmq.BaseRMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MessageConsumer extends BaseRMQ {

    private static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    public static void main(String[] args) throws Exception {
        String exchangeName = "directExchange";
        String queueName = "durableQueue";
        String routingKey = "myKey";

        Connection connection = createConnection(rmqHost, rmqPort, username, password, virtualHost);
        Channel channel = connection.createChannel();

        AMQP.Exchange.DeclareOk exchangeDeclareResult = channel.exchangeDeclare(exchangeName,
                BuiltinExchangeType.DIRECT, false);
        logger.info("Exchange {} is declared successfully with protocol method name {}."
                , exchangeName, exchangeDeclareResult.protocolMethodName());

        AMQP.Queue.DeclareOk declareResult = channel.queueDeclare(queueName, true, false, false, null);
        logger.info("Queue {} is declared successfully with consumer count {} and message count {}."
                , declareResult.getQueue(), declareResult.getConsumerCount(), declareResult.getMessageCount());

        Map<String, Object> headers = new HashMap<>();
        headers.put("color", "blue");
        AMQP.Queue.BindOk bindReulst = channel.queueBind(queueName, exchangeName, routingKey, headers);
        logger.info("Queue {} bind to exchange {} successfully with protocol method name {}.", queueName, exchangeName,
                bindReulst.protocolMethodName());
//        DefaultConsumer consumer = new DefaultConsumer(channel);
//        String consumerTag = channel.basicConsume(queueName, true, consumer);
//        logger.info("Receive consumer tag '{}' from server.", consumerTag);
//
//        while (true) {
//            consumer.handleDelivery();
//        }
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            logger.info("Receive message {} with consumer tag {}.", message, consumerTag);
//            throw new RuntimeException();
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }

}
