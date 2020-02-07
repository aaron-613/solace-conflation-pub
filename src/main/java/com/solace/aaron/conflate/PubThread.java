package com.solace.aaron.conflate;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class PubThread implements Runnable {

    
    private static final Logger logger = LogManager.getLogger("PubThread");

    final String pubID;
    final XMLMessageProducer producer;
    final PublishEventHandler streamingPubEventHandler;
    AtomicLong msgID = new AtomicLong(0);
    
    PubThread(String pubID, XMLMessageProducer producer, PublishEventHandler streamingPubEventHandler) {
        this.pubID = pubID;
        this.producer = producer;
        this.streamingPubEventHandler = streamingPubEventHandler;
    }
    
    @Override
    public void run() {
        try {
            while (true) {
//                  if (isConnected) {
                    BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                    byte[] payload = new byte[1000];
                    Arrays.fill(payload,(byte)0);
//                      msg.setData(("hello world "+msgID.incrementAndGet()).getBytes(UTF_8));
                    msg.setData(payload);
                    msg.setDeliveryMode(DeliveryMode.PERSISTENT);
                    msg.setCorrelationKey(msg);
//                      Topic topic = JCSMPFactory.onlyInstance().createTopic("a/b/c");
                    Queue queue = JCSMPFactory.onlyInstance().createQueue("q1");
                    try {
                        producer.send(msg,queue);
                        // send call returns successfully, that's good!
                        streamingPubEventHandler.addMessage(msg);
                    } catch (JCSMPException e) {
                        logger.error("Had an issue when trying to publish a message!!!",e);
                    }
//                  } else {
//                      logger.info("Not connected!  Not publishing!");
//                  }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            logger.info("This publishing thread got interrupted waiting",e);
        }
    }    

}
