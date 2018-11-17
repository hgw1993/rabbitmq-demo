package com.hgw.demo.DelayQueue;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * created 2018/9/13
 * author  guangwei.huang
 */
public class DeclareQueue {

    private static String EXCHANGE_NAME="notifyExchange";

    public static void init(){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        Connection connection;

        try {
            connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME,BuiltinExchangeType.TOPIC);

            String routingKey="AliPayNotify";
            //具体的消息体按照自己的要求封装成JSON数据
            String message = "http://localhost:8088/customerInfo/depositJsonList.do";

            channel.basicPublish(EXCHANGE_NAME,routingKey,null,message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        init();
    }

}
