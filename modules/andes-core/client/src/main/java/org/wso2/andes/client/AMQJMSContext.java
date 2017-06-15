/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.jms.JMSContext;
import org.wso2.andes.jms.MessageConsumer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * Implementation of JMSContext for JMS 2.0 Support.
 * One Context will have only one connection, but can have multiple sessions.
 */
public class AMQJMSContext extends Closeable implements AutoCloseable, JMSContext {

    private static final Logger logger = LoggerFactory.getLogger(AMQJMSContext.class);

    /**
     * The connection for the particular context.
     */
    private final AMQConnection connection;

    /**
     * The acknowledgement mode for the particular context.
     */
    private final int sessionMode;

    /**
     * Multiple sessions possible in one context.
     */
    private AMQSession session;
    private AtomicInteger sessionCount;

    /**
     * To specify whether the underlying connection used by this JMSContext
     * will be started automatically when a consumer is created.
     */
    private boolean autoStart = true;

    public AMQJMSContext(AMQConnection connection, int sessionMode) {
        this(connection, sessionMode, new AtomicInteger(1));
        logger.debug("Created JMSContext");
    }

    private AMQJMSContext(AMQConnection connection, int sessionMode, AtomicInteger sessionCount) {
        this.connection = connection;
        this.sessionMode = sessionMode;
        this.sessionCount = sessionCount;
    }

    @Override
    public synchronized JMSContext createContext(int sessionMode) {
        if (sessionCount.get() != 0) {
            sessionCount.incrementAndGet();
            if (logger.isDebugEnabled()) {
                logger.debug("Created new Context with session mode: {}", sessionMode);
            }
            return new AMQJMSContext(connection, sessionMode, sessionCount);
        } else {
            logger.error("Unable to create a new context: Connection Closed");
            throw new JMSRuntimeException("The Connection is Closed.");
        }
    }

    @Override
    public JMSProducer createProducer() {
        return new BasicJMSProducer((AMQSession_0_8) getSession());
    }

    @Override
    public String getClientID() {
        try {
            return connection.getClientID();
        } catch (JMSException e) {
            logger.error("Error retrieving client ID: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void setClientID(String clientID) {
        try {
            connection.setClientID(clientID);
        } catch (JMSException e) {
            logger.error("Error setting client ID: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public ConnectionMetaData getMetaData() {
        try {
            return connection.getMetaData();
        } catch (JMSException e) {
            logger.error("Error retrieving MetaData: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public ExceptionListener getExceptionListener() {
        try {
            return connection.getExceptionListener();
        } catch (JMSException e) {
            logger.error("Error retrieving Exception Listener: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        try {
            connection.setExceptionListener(listener);
        } catch (JMSException e) {
            logger.error("Error setting Exception Listener: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void start() {
        try {
            connection.start();
        } catch (JMSException e) {
            logger.error("Error starting/restarting delivery by JMScontext: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void stop() {
        try {
            connection.stop();
        } catch (JMSException e) {
            logger.error("Error temporarily stopping delivery by JMScontext: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    @Override
    public boolean getAutoStart() {
        return autoStart;
    }

    @Override
    public synchronized void close() {
        boolean isExceptionThrown = false;
        try {
            if (session != null) {
                session.close();
            }
        } catch (JMSException e) {
            isExceptionThrown = true;
            logger.error("Error closing session: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        } finally {
            try {
                if (sessionCount.decrementAndGet() == 0) {
                    connection.close();
                }
            } catch (JMSException e) {
                if (!isExceptionThrown) {
                    logger.error("Error closing connection: ", e);
                    throw new JMSRuntimeException(e.getMessage(), null, e);
                }
            }
        }
    }

    @Override
    public BytesMessage createBytesMessage() {
        try {
            return getSession().createBytesMessage();
        } catch (JMSException e) {
            logger.error("Error creating BytesMessage: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public MapMessage createMapMessage() {
        try {
            return getSession().createMapMessage();
        } catch (JMSException e) {
            logger.error("Error creating MapMessage: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public Message createMessage() {
        try {
            //TODO: Should create a Message object:currently creates a BytesMessage object
            return getSession().createMessage();
        } catch (JMSException e) {
            logger.error("Error creating Message: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage() {
        try {
            return getSession().createObjectMessage();
        } catch (JMSException e) {
            logger.error("Error creating ObjectMessage: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        try {
            return getSession().createObjectMessage(object);
        } catch (JMSException e) {
            logger.error("Error creating ObjectMessage with Serializable Object: {}",
                    new Object[]{object, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public StreamMessage createStreamMessage() {
        try {
            return getSession().createStreamMessage();
        } catch (JMSException e) {
            logger.error("Error creating StreamMessage: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        try {
            return getSession().createTextMessage();
        } catch (JMSException e) {
            logger.error("Error creating TextMessage: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public TextMessage createTextMessage(String text) {
        try {
            return getSession().createTextMessage(text);
        } catch (JMSException e) {
            logger.error("Error creating TextMessage with String: {}", new Object[]{text, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public boolean getTransacted() {
        return sessionMode == Session.SESSION_TRANSACTED;
    }

    @Override
    public int getSessionMode() {
        return sessionMode;
    }

    @Override
    public void commit() {
        try {
            getSession().commit();
        } catch (IllegalStateException e) {
            logger.error("Error: Trying to Commit in Session that is Not Transacted: ", e);
            throw new IllegalStateRuntimeException(e.getMessage(), null, e);
        } catch (JMSException e) {
            logger.error("Error Committing: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void rollback() {
        try {
            getSession().rollback();
        } catch (IllegalStateException e) {
            logger.error("Error: Trying to Rollback in Session that is Not Transacted: ", e);
            throw new IllegalStateRuntimeException(e.getMessage(), null, e);
        } catch (JMSException e) {
            logger.error("Error Rolling Back: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void recover() {
        try {
            getSession().recover();
        } catch (IllegalStateException e) {
            logger.error("Error: Trying to Recover in Session that is Transacted: ", e);
            throw new IllegalStateRuntimeException(e.getMessage(), null, e);
        } catch (JMSException e) {
            logger.error("Error Recovering: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer = new BasicJMSConsumer((BasicMessageConsumer_0_8)
                    currentSession.createConsumer(destination));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Consumer for {}", destination);
            }
            return jmsConsumer;
        } catch (JMSException e) {
            logger.error("Error Creating Consumer: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer = new BasicJMSConsumer((BasicMessageConsumer_0_8)
                    currentSession.createConsumer(destination, messageSelector));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Consumer for: {} with Selector: {}",
                        new Object[]{destination, messageSelector});
            }
            return jmsConsumer;
        } catch (JMSException e) {
            logger.error("Error Creating Consumer for: {} with Selector: {}",
                    new Object[]{destination, messageSelector, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector,
                                      boolean noLocal) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer = new BasicJMSConsumer((BasicMessageConsumer_0_8)
                    currentSession.createConsumer(destination, messageSelector, noLocal));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Consumer for: {} with Selector: {} and noLocal: {}",
                        new Object[]{destination, messageSelector, noLocal});
            }
            return jmsConsumer;
        } catch (JMSException e) {
            logger.error("Error Creating Consumer for: {} with Selector: {} and noLocal: {}",
                    new Object[]{destination, messageSelector, noLocal, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public Queue createQueue(String queueName) {
        try {
            Queue queue = getSession().createQueue(queueName);
            if (logger.isDebugEnabled()) {
                logger.debug("Created Queue: {}", queueName);
            }
            return queue;
        } catch (JMSException e) {
            logger.error("Error creating queue: {}", new Object[]{queueName, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public Topic createTopic(String topicName) {
        try {
            Topic topic = getSession().createTopic(topicName);
            if (logger.isDebugEnabled()) {
                logger.debug("Created Topic: {}", topicName);
            }
            return topic;
        } catch (JMSException e) {
            logger.error("Error creating topic: {}", new Object[]{topicName, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer = new BasicJMSConsumer((TopicSubscriberAdaptor)
                    currentSession.createDurableConsumer(topic, name));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Durable Consumer for: {}", topic);
            }
            return jmsConsumer;
        } catch (JMSException e) {
            logger.error("Error Creating Durable Consumer for: {}", new Object[]{topic, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector,
                                             boolean noLocal) {
        AMQSession_0_8 currentSession = (AMQSession_0_8) getSession();
        try {
            startConnectionIfRequired();
            BasicJMSConsumer jmsConsumer = new BasicJMSConsumer((TopicSubscriberAdaptor)
                    currentSession.createDurableConsumer(topic, name, messageSelector, noLocal));
            if (logger.isDebugEnabled()) {
                logger.debug("Created Durable Consumer for: {} with Selector: {} ",
                        new Object[]{topic, messageSelector});
            }
            return jmsConsumer;
        } catch (JMSException e) {
            logger.error("Error Creating Durable Consumer for: {} with Selector: {} ",
                    new Object[]{topic, messageSelector, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name,
                                                   String messageSelector) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName,
                                            String messageSelector) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        try {
            QueueBrowser queueBrowser = getSession().createBrowser(queue);
            if (logger.isDebugEnabled()) {
                logger.debug("Created Browser for: {}", queue);
            }
            return queueBrowser;
        } catch (JMSException e) {
            logger.error("Error Creating Browser for Queue: {}", new Object[]{queue, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        try {
            QueueBrowser queueBrowser = getSession().createBrowser(queue, messageSelector);
            if (logger.isDebugEnabled()) {
                logger.debug("Created Browser for: {} with Selector: {}",
                        new Object[]{queue, messageSelector});
            }
            return queueBrowser;
        } catch (JMSException e) {
            logger.error("Error Creating Browser for: {} with Selector: {}",
                    new Object[]{queue, messageSelector, e});
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        try {
            TemporaryQueue temporaryQueue = getSession().createTemporaryQueue();
            logger.debug("Created Temporary Queue");
            return temporaryQueue;
        } catch (JMSException e) {
            logger.error("Error Creating Temporary Queue: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        try {
            TemporaryTopic temporaryTopic = getSession().createTemporaryTopic();
            logger.debug("Created Temporary Topic");
            return temporaryTopic;
        } catch (JMSException e) {
            logger.error("Error Creating Temporary Topic: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    /**
     * TODO: Should throw InvalidDestinationRuntimeException if invalid name.
     *
     * @param name name used to identify this subscription
     */
    @Override
    public void unsubscribe(String name) {
        try {
            getSession().unsubscribe(name);
        } catch (JMSException e) {
            logger.error("Error: Failed to Unsubscribe: ", e);
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void acknowledge() {
        if (getSessionMode() == Session.CLIENT_ACKNOWLEDGE) {
            try {
                getSession().acknowledge();
            } catch (IllegalStateException e) {
                logger.error("Error: Trying to Acknowledge in Closed Context: ", e);
                throw new IllegalStateRuntimeException(e.getMessage(), null, e);
            }
        }
    }

    /**
     * Custom Helper Methods
     */
    /**
     * Method to retrieve session.
     *
     * @return session currently applicable session
     */
    private AMQSession getSession() {
        if (session == null) {
            synchronized (this) {
                if (session == null) {
                    try {
                        session = (AMQSession) connection.createSession(getSessionMode());
                    } catch (JMSException e) {
                        logger.error("Error Creating Session: ", e);
                        throw new JMSRuntimeException(e.getMessage(), null, e);
                    }
                }
            }
        }
        return session;
    }

    /**
     * According to the spec: connection needs to be started automatically when a consumer is
     * created if autoStart is set to true.
     */
    private void startConnectionIfRequired() {
        if (getAutoStart()) {
            try {
                if (!connection._started) {
                    logger.debug("Connection Started");
                    connection.start();
                }
            } catch (JMSException e) {
                logger.error("Error Starting Connection: ", e);
                throw new JMSRuntimeException(e.getMessage(), null, e);
            }
        }
    }
}
