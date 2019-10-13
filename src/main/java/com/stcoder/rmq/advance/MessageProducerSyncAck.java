package com.stcoder.rmq.advance;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.stcoder.rmq.BaseRMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducerSyncAck extends BaseRMQ {

    private static Logger logger = LoggerFactory.getLogger(MessageProducerSyncAck.class);

    public static void main(String[] args) throws Exception {
        String exchangeName = "directExchange";
        String queueName = "directQueue";
        String routingKey = "myKey";

        Connection connection = createConnection(rmqHost, rmqPort, username, password, virtualHost);
        Channel channel = connection.createChannel();
        channel.confirmSelect();

        AMQP.Exchange.DeclareOk exchangeDeclareResult = channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
        logger.info("Exchange {} is declared successfully with protocol method name {}."
                , exchangeName, exchangeDeclareResult.protocolMethodName());

        AMQP.Queue.DeclareOk queueDeclareResult = channel.queueDeclare(queueName, true, false, false, null);
        logger.info("Queue {} is declared successfully with consumer count {} and message count {}."
                , queueDeclareResult.getQueue(), queueDeclareResult.getConsumerCount(), queueDeclareResult.getMessageCount());

        String message = "Hello World!";

        channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
        if (channel.waitForConfirms(10 * 1000L)) {
            logger.info("Publish message successfully");
        } else {
            logger.info("Fail to publish message");
        }

        channel.close();
        connection.close();
    }

}
