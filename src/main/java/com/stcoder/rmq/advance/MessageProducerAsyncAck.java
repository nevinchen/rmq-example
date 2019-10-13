package com.stcoder.rmq.advance;

import com.rabbitmq.client.*;
import com.stcoder.rmq.BaseRMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MessageProducerAsyncAck extends BaseRMQ {

    private static Logger logger = LoggerFactory.getLogger(MessageProducerAsyncAck.class);

    public static void main(String[] args) throws Exception {
        String exchangeName = "directExchange";
        String queueName = "directQueue";
        String routingKey = "myKey";

        Connection connection = createConnection(rmqHost, rmqPort, username, password, virtualHost);
        Channel channel = connection.createChannel();
        channel.confirmSelect();
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                logger.info("Receive producer confirm Ack with deliveryTag {} and multiple {}.", deliveryTag, multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                logger.info("Receive producer confirm Nack with deliveryTag {} and multiple {}.", deliveryTag, multiple);
            }
        });

        AMQP.Exchange.DeclareOk exchangeDeclareResult = channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
        logger.info("Exchange {} is declared successfully with protocol method name {}."
                , exchangeName, exchangeDeclareResult.protocolMethodName());

        AMQP.Queue.DeclareOk queueDeclareResult = channel.queueDeclare(queueName, true, false, false, null);
        logger.info("Queue {} is declared successfully with consumer count {} and message count {}."
                , queueDeclareResult.getQueue(), queueDeclareResult.getConsumerCount(), queueDeclareResult.getMessageCount());

        String message = "Hello World!";

        channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
        logger.info("Publish message successfully");

        //Sleep to wait for RMQ callback
        Thread.sleep(10 * 1000L);

        channel.close();
        connection.close();
    }

}
