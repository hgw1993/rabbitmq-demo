package com.example.consumer.delayQueue;

import com.rabbitmq.client.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.HttpStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * created 2018/9/13
 * author  guangwei.huang
 */
public class DecalreConsumer {
    private static String EXCHANGE_NAME = "notifyExchange";

    private final String routingKey = "AliPayNotify";

    private final String QUEUE_DEAD_NAME = "deadLetterQueue";

    private String originalExpiration = "0";

    public String QU_declare_15S = "Qu_declare_15s";
    public String EX_declare_15S = "EX_declare_15s";

    private Channel channel;

    public void init() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        Connection connection;

        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    private void consumer() {
        try {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            channel.queueDeclare(QUEUE_DEAD_NAME,false,false,false,null);
            channel.queueBind(QUEUE_DEAD_NAME, EXCHANGE_NAME, routingKey);


            final Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    Map<String, Object> headers = properties.getHeaders();
                    if (headers != null) {
                        List<Map<String, Object>> xDeath = (List<Map<String, Object>>) headers.get("x-death");
                        if (xDeath != null && xDeath.size() > 0) {
                            Map<String, Object> entrys = xDeath.get(0);
                            originalExpiration = entrys.get("original-expiration").toString();
                            System.out.println("originalExpiration is " +originalExpiration);
                        }
                    }

                    sendRestNotice(message);
                    channel.basicAck(envelope.getDeliveryTag(),true);
                }
            };
            //第二个参数值为false代表关闭RabbitMQ的自动应答机制，改为手动应答
            channel.basicConsume(QUEUE_DEAD_NAME, false, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendRestNotice(String url) {
        StringBuffer responseBody = null;
        try {
            HttpClient client = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(url);
            HttpResponse response = client.execute(post);
            BufferedReader reader;
            if (response.getStatusLine().getStatusCode() == HttpStatus.OK.value()) {
                reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "utf-8"));
                responseBody = new StringBuffer();
                String line;
                while ((line = reader.readLine()) != null) {
                    responseBody.append(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            if (responseBody == null || !"success".equals(responseBody)) {
                if (originalExpiration.equals("0")) {
                    putDeclareQueue(url, 3000, QU_declare_15S);
                }
                if (originalExpiration.equals("3000")) {
                    putDeclareQueue(url, 30000, QU_declare_15S);
                }
                if (originalExpiration.equals("30000")) {
                    putDeclareQueue(url, 60000, QU_declare_15S);
                }
                if (originalExpiration.equals("60000")) {
                    putDeclareQueue(url, 120000, QU_declare_15S);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void putDeclareQueue(String message, int millSeconds, String queueName) throws IOException {
        channel.exchangeDeclare(EX_declare_15S, BuiltinExchangeType.TOPIC);
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", EXCHANGE_NAME);
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        //设置消息的过期时间
        builder.expiration(String.valueOf(millSeconds));
        builder.deliveryMode(2);
        AMQP.BasicProperties properties = builder.build();

        channel.queueDeclare(queueName, false, false, false, args);

        channel.queueBind(queueName, EX_declare_15S, routingKey);

        channel.basicPublish(EX_declare_15S, routingKey, properties, message.getBytes());

        System.out.println("send message in " + queueName + message + "time============" + System.currentTimeMillis());
    }

    public static void main(String[] args) {
        DecalreConsumer consumer = new DecalreConsumer();

        consumer.init();
        consumer.consumer();
    }
}

