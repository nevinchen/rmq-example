package com.stcoder.rmq.advance;

import com.rabbitmq.client.*;
import com.stcoder.rmq.BaseRMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducerDurable extends BaseRMQ {

    private static Logger logger = LoggerFactory.getLogger(MessageProducerDurable.class);

    public static void main(String[] args) throws Exception {
        String exchangeName = "directExchange";
        String queueName = "durableQueue";
        String routingKey = "myKey";

        Connection connection = createConnection(rmqHost, rmqPort, username, password, virtualHost);
        Channel channel = connection.createChannel();
        //Create a durable exchange
        AMQP.Exchange.DeclareOk exchangeDeclareResult = channel.exchangeDeclare(exchangeName,
                BuiltinExchangeType.DIRECT, false);
        logger.info("Exchange {} is declared successfully with protocol method name {}."
                , exchangeName, exchangeDeclareResult.protocolMethodName());

        //Create a durable queue and exclusive must be false
        AMQP.Queue.DeclareOk queueDeclareResult = channel.queueDeclare(queueName, true, false, false, null);
        logger.info("Queue {} is declared successfully with consumer count {} and message count {}."
                , queueDeclareResult.getQueue(), queueDeclareResult.getConsumerCount(), queueDeclareResult.getMessageCount());

        String message = "Hello World!";

        channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        logger.info("Publish message successfully");

        channel.close();
        connection.close();
    }

}
