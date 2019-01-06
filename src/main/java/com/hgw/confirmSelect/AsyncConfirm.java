package com.hgw.confirmSelect;

import com.hgw.common.ChannelFactory;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *  发送确认机制--异步确认
 */
public class AsyncConfirm {

    private final String EXCHANGE_NAME="hgw1";
    private final String QUENE_NAME= "que_hgw1";
    private final String ROUTE_KEY = "hgw1_key";
    //采用有序集合存储
    SortedSet sortedSet = new TreeSet();
    Channel channel = ChannelFactory.getChannel();

    public void init(){
        try {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            channel.queueDeclare(QUENE_NAME, false, false, false, null);
            channel.queueBind(QUENE_NAME,EXCHANGE_NAME,ROUTE_KEY);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void asyncConfirm(){
        if(channel == null){
            throw new RuntimeException("channel is null");
        }
        try {
            channel.confirmSelect();
            //异步确认监听
            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    //multiple 为true时，表示到这个序号之前的所有消息都已经处理
                    if(multiple){

                    }else{
                        sortedSet.remove(deliveryTag);
                    }
                    System.out.println("ACK SeqNo is "+ deliveryTag +" multiple is "+multiple+" len is "+sortedSet.size());
                }
                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println("nack  SeqNo is "+ deliveryTag +" multiple is "+multiple+" len is "+sortedSet.size());
                        sortedSet.remove(deliveryTag);
                    //注意处理消息重发的场景
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void publishMsg(){
        try {
            long seqNo = channel.getNextPublishSeqNo();
            channel.basicPublish(EXCHANGE_NAME,ROUTE_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN,"this is test message".getBytes());
            sortedSet.add(seqNo);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        AsyncConfirm confirm = new AsyncConfirm();
//        confirm.init();
        confirm.asyncConfirm();
        for (int i=0; i<10;i++){
            confirm.publishMsg();
        }
    }

}
