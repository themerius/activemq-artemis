/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;

/**
 * An example using StompJMS.
 */
public class StompExample {

   public static void main(final String[] args) throws Exception {
      StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
      factory.setQueuePrefix("jms.queue.");
      factory.setDisconnectTimeout(5000);
      factory.setTopicPrefix("jms.topic.");
      factory.setBrokerURI("tcp://localhost:61616");
      Connection connection = factory.createConnection();
      connection.start();

      final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue("queue1");
      final MessageProducer producer = session.createProducer(queue);
      MessageConsumer consumer = session.createConsumer(queue);

      producer.send(session.createTextMessage("Hello"));


      System.out.println("Waiting 10 seconds");
      Thread.sleep(10000); // increase this and it will fail
      System.out.println("waited");

      // send message all second.
      if (System.getProperty("traffic") != null) {
          Timer timer = new Timer();
          timer.schedule( new TimerTask() {
              public void run() {
                  try {
                      producer.send(session.createTextMessage("Hello from Timer @ " + new Date()));
                  } catch (Exception e) {
                      System.out.println("Oops");
                  }
              }
          }, 0, 1000);
      }

      // block until messages arrive.
      // every time a message arrives, process it
      while (true) {
        TextMessage message = (TextMessage) consumer.receive();
        System.out.println("The content of the message is " + message.getText());
      }
   }
}
