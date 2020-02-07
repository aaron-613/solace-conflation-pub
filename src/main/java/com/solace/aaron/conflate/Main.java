package com.solace.aaron.conflate;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProducerEventHandler;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPReconnectEventHandler;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.ProducerEventArgs;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;



public class Main {

    static Properties appProperties;
    
    
    /**
     * However you want to load your properties.  Could be via a database, command-line params, anything.
     * @param properties
     * @throws IllegalArgumentException
     */
    void loadProperties(InputStream inputStream) throws IllegalArgumentException {
        
        appProperties = new Properties();
        try {
            appProperties.load(inputStream);
        } catch (IOException e) {
            
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
        
    }
    
    
    ScheduledExecutorService service = Executors.newScheduledThreadPool(10);
    /* SOLACE VARIABLES */
    JCSMPSession session;
    JCSMPProperties properties;
    XMLMessageProducer producer;
    XMLMessageConsumer consumer;
    PublishEventHandler streamingPubEventHandler;
    volatile boolean isConnected = false;
    
    private static final Logger logger = LogManager.getLogger("Main");
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    
    
    public void initializeProperties() throws InvalidPropertiesException {
        properties = new JCSMPProperties();
        // init
        properties.setProperty(JCSMPProperties.HOST,"localhost");
        properties.setProperty(JCSMPProperties.VPN_NAME,"default");
        properties.setProperty(JCSMPProperties.USERNAME,"default");
        properties.setProperty(JCSMPProperties.PASSWORD,"default");
        properties.setProperty(JCSMPProperties.APPLICATION_DESCRIPTION,"A test client for testing things");
        properties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE,2);  // I can have 50 Guaranteed messages in-flight, for performance!b
        
        try {
            session = JCSMPFactory.onlyInstance().createSession(properties,JCSMPFactory.onlyInstance().getDefaultContext(),sessionEventHandler);
            
        } catch (InvalidPropertiesException e) {
            // log about how this is is bad news
            throw e;
        }
    }
    
    SessionEventHandler sessionEventHandler = new SessionEventHandler() {
		
		@Override
		public void handleEvent(SessionEventArgs arg0) {
			logger.warn("WOW what just happened????? "+arg0);
		}
	};
    
//    JCSMPStreamingPublishCorrelatingEventHandler streamingPubEventHandler = new JCSMPStreamingPublishCorrelatingEventHandler() {
        
    
    
    
    JCSMPProducerEventHandler producerEventHandler = new JCSMPProducerEventHandler() {
        
        @Override
        public void handleEvent(ProducerEventArgs arg0) {
        	logger.error("I can't believe this got called!? "+arg0);
        }
    };
    
    JCSMPReconnectEventHandler reconnectEventHandler = new JCSMPReconnectEventHandler() {
        
        @Override
        public boolean preReconnect() throws JCSMPException {
            if (isConnected) {  // don't worry about multi-thread access or Atomic, only one thread will call this
                logger.info("Disconnected!");
                isConnected = false;
            }
        	logger.info("About to attempt a reconnect attempt!");
            // log you are about to attempt to reconnect, do you want to continue?
            return true;
        }
        
        @Override
        public void postReconnect() throws JCSMPException {
        	logger.info("Successfully reconnected to Solace!");
            // you've reconnected!!!!
        	isConnected = true;
        }
    };
    
    XMLMessageListener messageListener = new XMLMessageListener() {
        
        @Override
        public void onReceive(BytesXMLMessage arg0) {
            // The callback method to handle messages received by the XMLMessageConsumer.

            
        }
        
        @Override
        public void onException(JCSMPException arg0) {
            // On error, this method is invoked. In general, the error is unrecoverable.
            
        }
    };
    
    
    public void run() {
        try {
            initializeProperties();
                        
            producer = session.getMessageProducer(streamingPubEventHandler,producerEventHandler);
            consumer = session.getMessageConsumer(reconnectEventHandler,messageListener);
            streamingPubEventHandler = new PublishEventHandler();
            session.connect();
            
            isConnected = true;
            logger.info("Pub ACK window size: "+session.getProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE));
            logger.info("Pub ACK time: "+session.getProperty(JCSMPProperties.PUB_ACK_TIME));
            
            
            consumer.start();
            
            service.schedule(new PubThread("1"),0,TimeUnit.SECONDS);

        } catch (InvalidPropertiesException e) {
            // log that we cannot continue
            return;
        } catch (JCSMPException e) {
            
        }
        
        
    }
    
    class PubThread implements Runnable {
		
    	final String pubID;
    	AtomicLong msgID = new AtomicLong(0);
    	
    	PubThread(String pubID) {
    		this.pubID = pubID;
    	}
    	
		@Override
		public void run() {
			try {
				while (true) {
//					if (isConnected) {
						BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
						byte[] payload = new byte[1000];
						Arrays.fill(payload,(byte)0);
//						msg.setData(("hello world "+msgID.incrementAndGet()).getBytes(UTF_8));
						msg.setData(payload);
						msg.setDeliveryMode(DeliveryMode.PERSISTENT);
						msg.setCorrelationKey(msg);
//						Topic topic = JCSMPFactory.onlyInstance().createTopic("a/b/c");
						Queue queue = JCSMPFactory.onlyInstance().createQueue("q1");
						try {
							producer.send(msg,queue);
							// send call returns successfully, that's good!
							streamingPubEventHandler.addMessage(msg);
						} catch (JCSMPException e) {
							logger.error("Had an issue when trying to publish a message!!!",e);
						}
//					} else {
//						logger.info("Not connected!  Not publishing!");
//					}
					Thread.sleep(1000);
				}
			} catch (InterruptedException e) {
				logger.info("This publishing thread got interrupted waiting",e);
			}
		}
	};
    
    
    public static void main(String... args) throws InvalidPropertiesException {
        
        if (args.length < 1) {
            System.err.println("You must specifiy at least one argument, the name of the properties file to load");
            System.err.println();
            System.exit(-1);
        }
        Main main = new Main();
        
        try {
            InputStream fis = new FileInputStream(args[0]);
            main.loadProperties(fis);
        } catch (FileNotFoundException e) {
            System.err.printf("Could not find the specified file '%s'. Please check",args[0]);
            System.err.println();
            //System.exit(-2);
        }
        
        main.initializeProperties();
        main.run();
    	try {
            while (true) {
				Thread.sleep(100000);
            }
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
    
}
