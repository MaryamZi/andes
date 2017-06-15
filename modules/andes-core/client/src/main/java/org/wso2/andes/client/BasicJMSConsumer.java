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

import org.wso2.andes.jms.JMSConsumer;

import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

/**
 * andes Implementation of JMSConsumer.
 */
public class BasicJMSConsumer extends Closeable implements JMSConsumer {

    private final MessageConsumer messageConsumer;

    private org.wso2.andes.nclient.util.MessageListener messageListener = null;

    /**
     * Create a new BasicJMSConsumer instance.
     *
     * @param messageConsumer The underlying MessageConsumer for this JMSConsumer
     */
    public BasicJMSConsumer(BasicMessageConsumer_0_8 messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    /**
     * Create a new BasicJMSConsumer instance with a TopicSubscriber.
     *
     * @param topicSubscriberAdaptor The underlying TopicSubscriber for this JMSConsumer
     */
    public BasicJMSConsumer(TopicSubscriberAdaptor topicSubscriberAdaptor) {
        this.messageConsumer = topicSubscriberAdaptor;
    }

    @Override
    public String getMessageSelector() throws JMSRuntimeException {
        try {
            return messageConsumer.getMessageSelector();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        try {
            return messageConsumer.getMessageListener();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSRuntimeException {
        try {
            messageConsumer.setMessageListener(messageListener);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public Message receive() throws JMSRuntimeException {
        try {
            return messageConsumer.receive();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public Message receive(long timeout) throws JMSRuntimeException {
        try {
            return messageConsumer.receive(timeout);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public Message receiveNoWait() {
        try {
            return messageConsumer.receiveNoWait();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void close() throws JMSRuntimeException {
        _closed.set(true);
        try {
            messageConsumer.close();
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> c) throws JMSRuntimeException {
        try {
            Message message = messageConsumer.receive();
            if (message == null) {
                return null;
            } else {
                T messageBody = message.getBody(c);
                return messageBody;
            }

        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) throws JMSRuntimeException {
        try {
            Message message = messageConsumer.receive(timeout);
            if (message == null) {
                return null;
            } else {
                T messageBody = message.getBody(c);
                return messageBody;
            }

        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        try {
            Message message = messageConsumer.receiveNoWait();
            if (message == null) {
                return null;
            } else {
                T messageBody = message.getBody(c);
                return messageBody;
            }

        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }
}
