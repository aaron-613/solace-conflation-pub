package com.solace.aaron.conflate;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.XMLMessage;

public class PublishEventHandler implements JCSMPStreamingPublishEventHandler {

    private static final Logger logger = LogManager.getLogger("PublishEventHandler");
    
    LinkedBlockingQueue<XMLMessage> publishedMessages = new LinkedBlockingQueue<XMLMessage>();

    
    void addMessage(XMLMessage msg) throws InterruptedException {
        publishedMessages.put(msg);
    }
    
    @Override
    public void responseReceived(String messageID) {
        // will never get called, since I've implemented the JCSMPStreamingPublishCorrelatingEventHandler interface
        logger.info("ACK "+messageID);
        XMLMessage msg;
        try {
            msg = publishedMessages.take();  // blocking call!
//          if (!messageID.equals(msg.getMessageId())) {
//              logger.warn("Difference!!!! msgID="+messageID+" vs. queue.msg.msgID="+msg.getAckMessageId());
//          }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

//  @Override
    public void responseReceivedEx(Object key) {
        logger.info("ACK "+((XMLMessage)key).getMessageId());
        XMLMessage msg;
        try {
            msg = publishedMessages.take();  // blocking call!
            if (!key.equals(msg)) {
                logger.warn("Difference!!!!");
                logger.warn(key);
                logger.warn(msg);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleError(String messageID, JCSMPException cause, long timestamp) {
        // will never get called, since I've implemented the JCSMPStreamingPublishCorrelatingEventHandler interface
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append("*** NACK *** ");
        if (messageID != null) {
            XMLMessage msg;
            try {
                long start = System.currentTimeMillis();
                msg = publishedMessages.take();  // take does a blocking call, rather than poll() which returns null possibly in a race condition
                if ((System.currentTimeMillis()-start) > 50) {
                    logger.warn("It took WAAAAY too long for that take() call to return: "+(System.currentTimeMillis()-start)+" ms");
                }
//              if (!messageID.equals(msg.getMessageId())) {
//                  logger.warn("Difference!!!! msgID="+messageID+" vs. queue.msg.msgID="+msg.getAckMessageId());
//              }
            } catch (InterruptedException e) {
                System.err.println("HOLY SHIT! this got thrown when trying to take a message off the publishing queue");
                e.printStackTrace();
            }
            errorMsg.append(messageID+" - ");
        } else {
            errorMsg.append("NULL - ");
        }
        if (cause instanceof JCSMPErrorResponseException) {
            cause = (JCSMPErrorResponseException)cause;
            JCSMPErrorResponseException respEx = (JCSMPErrorResponseException)cause;

            errorMsg.append(String.format("%s",respEx.getMessages()));//toString()));
        } else {
            errorMsg.append(cause);
        }
        logger.error(errorMsg.toString());
    }

//  @Override
    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append("*** NACK *** ");
        if (key != null) {
            XMLMessage msg;
            try {
                msg = publishedMessages.take();  // take does a blocking call, rather than poll() which returns null possibly in a race condition
                if (!key.equals(msg)) {
                    logger.warn("Difference!!!!");
                    logger.warn(key);
                    logger.warn(msg);
                }
            } catch (InterruptedException e) {
                System.err.println("HOLY SHIT! this got thrown when trying to take a message off the publishing queue");
                e.printStackTrace();
            }
            errorMsg.append(((XMLMessage)key).getMessageId()+" - ");
        } else {
            errorMsg.append("NULL - ");
        }
        if (cause instanceof JCSMPErrorResponseException) {
            cause = (JCSMPErrorResponseException)cause;
            JCSMPErrorResponseException respEx = (JCSMPErrorResponseException)cause;

            errorMsg.append(String.format("%s",respEx.getMessages()));//toString()));
        } else {
            errorMsg.append(cause);
        }
        logger.error(errorMsg.toString());
    }


}
