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

import org.wso2.andes.jms.JMSProducer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

/**
 * andes Implementation of JMSProducer.
 */
public class BasicJMSProducer extends Closeable implements JMSProducer {

    private final AMQSession_0_8 session;
    private final BasicMessageProducer_0_8 messageProducer;

    //TODO: use Delivery Delay, DisableMessageID, etc.
    private boolean disableMessageID = false;
    private boolean disableMessageTimestamp = false;
    private CompletionListener completionListener = null;
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private int priority = Message.DEFAULT_PRIORITY;
    private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    private String jmsCorrelationID;
    private String jmsType;
    private Destination jmsReplyTo;
    private byte[] jmsCorrelationIDAsBytes;
    private Map<String, Object> messageProperties = new HashMap<String, Object>();

    /**
     * Create a new BasicJMSProducer instance.
     *
     * @param session The Session that created this JMSProducer
     */
    public BasicJMSProducer(AMQSession_0_8 session) {
        this.session = session;
        try {
            messageProducer = session.createProducer(null);
        } catch (IllegalStateException e) {
            throw new IllegalStateRuntimeException(e.getMessage(), null, e);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }


    @Override
    public javax.jms.JMSProducer send(Destination destination, Message message) {
        try {
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
        return this;
    }

    @Override
    public javax.jms.JMSProducer send(Destination destination, String body) {
        try {
            TextMessage message = session.createTextMessage(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
        return this;
    }

    @Override
    public javax.jms.JMSProducer send(Destination destination, Map<String, Object> body) {
        try {
            MapMessage message = session.createMapMessage();
            for (Map.Entry<String, Object> mapEntry : body.entrySet()) {
                message.setObject(mapEntry.getKey(), mapEntry.getValue());
            }
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
        return this;
    }

    @Override
    public javax.jms.JMSProducer send(Destination destination, byte[] body) {
        try {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
        return this;
    }

    @Override
    public javax.jms.JMSProducer send(Destination destination, Serializable body) {
        try {
            ObjectMessage message = session.createObjectMessage();
            message.setObject(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
        return this;
    }

    @Override
    public javax.jms.JMSProducer setDisableMessageID(boolean value) {
        disableMessageID = value;
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        return disableMessageID;
    }

    @Override
    public javax.jms.JMSProducer setDisableMessageTimestamp(boolean value) {
        disableMessageTimestamp = value;
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    @Override
    public javax.jms.JMSProducer setDeliveryMode(int deliveryMode) {
        if (deliveryMode == DeliveryMode.NON_PERSISTENT) {
            this.deliveryMode = deliveryMode;
        } else if (deliveryMode != DeliveryMode.PERSISTENT) {
            throw new JMSRuntimeException("Invalid Delivery Mode " + deliveryMode);
        }
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public javax.jms.JMSProducer setPriority(int i) {
        if (i >= 0 && i <= 9) {
            priority = 1;
        } else {
            throw new JMSRuntimeException("Invalid Priority Level: " + i + ", Valid Range: 0-9");
        }
        return this;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public javax.jms.JMSProducer setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public javax.jms.JMSProducer setDeliveryDelay(long deliveryDelay) {
        this.deliveryDelay = deliveryDelay;
        return this;
    }

    @Override
    public long getDeliveryDelay() {
        return deliveryDelay;
    }

    @Override
    public Destination getDestination() throws JMSException {
        return messageProducer.getDestination();
    }

    @Override
    public javax.jms.JMSProducer setAsync(CompletionListener completionListener) {
        this.completionListener = completionListener;
        return this;
    }

    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, boolean value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, byte value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, short value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, int value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, long value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, float value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, double value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, String value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer setProperty(String name, Object value) {
        return setMessageProperty(name, value);
    }

    @Override
    public javax.jms.JMSProducer clearProperties() {
        messageProperties.clear();
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        return messageProperties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public byte getByteProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public short getShortProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public int getIntProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public long getLongProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public float getFloatProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public double getDoubleProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public String getStringProperty(String name) {
        throw new JmsNotImplementedRuntimeException();
    }

    @Override
    public Object getObjectProperty(String name) {
        return messageProperties.get(name);
    }

    @Override
    public Set<String> getPropertyNames() {
        return messageProperties.keySet();
    }

    @Override
    public javax.jms.JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        jmsCorrelationIDAsBytes = correlationID;
        return this;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return jmsCorrelationIDAsBytes;
    }

    @Override
    public javax.jms.JMSProducer setJMSCorrelationID(String correlationID) {
        jmsCorrelationID = correlationID;
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return jmsCorrelationID;
    }

    @Override
    public javax.jms.JMSProducer setJMSType(String type) {
        jmsType = type;
        return this;
    }

    @Override
    public String getJMSType() {
        return jmsType;
    }

    @Override
    public javax.jms.JMSProducer setJMSReplyTo(Destination replyTo) {
        jmsReplyTo = replyTo;
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return jmsReplyTo;
    }

    @Override
    public void close() {
        _closed.set(true);
        messageProducer.close();
    }

    /* Custom Methods */

    private void sendMessage(Destination destination, Message message) throws JMSException {
        if (message == null) {
            throw new MessageFormatRuntimeException("Message Cannot Be Null");
        }
        if (destination == null) {
            throw new InvalidDestinationRuntimeException("Destination Cannot Be Null");
        }

        for (Map.Entry<String, Object> entry : messageProperties.entrySet()) {
            message.setObjectProperty(entry.getKey(), entry.getValue());
        }

        //TODO: if message properties read-only --> MessageNotWriteableRuntimeException
        if (jmsCorrelationID != null) {
            message.setJMSCorrelationID(jmsCorrelationID);
        }
        if (jmsCorrelationIDAsBytes != null) {
            message.setJMSCorrelationIDAsBytes(jmsCorrelationIDAsBytes);
        }
        if (jmsReplyTo != null) {
            message.setJMSReplyTo(jmsReplyTo);
        }
        if (jmsType != null) {
            message.setJMSType(jmsType);
        }

        try {
            messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }


    private javax.jms.JMSProducer setMessageProperty(String name, Object value) {
        try {
            if (name != null && name.length() > 0) {
                checkValueValidity(value);
                messageProperties.put(name, value);
                return this;
            } else {
                throw new IllegalArgumentException("Name Cannot be Null or Empty.");
            }
        } catch (MessageFormatRuntimeException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    private void checkValueValidity(Object value) throws MessageFormatRuntimeException {
        boolean isValid = false;
        if (value instanceof Boolean || value instanceof Byte || value instanceof Character ||
                value instanceof Double || value instanceof Float || value instanceof Integer ||
                value instanceof Long || value instanceof Short || value instanceof String ||
                value == null) {
            isValid = true;
        }

        if (!isValid) {
            throw new MessageFormatRuntimeException("Values of Type " + value.getClass()
                    + " are Not Allowed");
        }
    }
}
