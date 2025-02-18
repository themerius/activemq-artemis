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
package org.apache.activemq.artemis.core.protocol.stomp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.protocol.stomp.v10.StompFrameHandlerV10;
import org.apache.activemq.artemis.core.protocol.stomp.v12.StompFrameHandlerV12;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.VersionLoader;

import static org.apache.activemq.artemis.core.protocol.stomp.ActiveMQStompProtocolMessageBundle.BUNDLE;

public final class StompConnection implements RemotingConnection {

   protected static final String CONNECTION_ID_PROP = "__AMQ_CID";
   private static final String SERVER_NAME = "ActiveMQ-Artemis/" + VersionLoader.getVersion().getFullVersion() +
      " ActiveMQ Artemis Messaging Engine";

   private final StompProtocolManager manager;

   private final Connection transportConnection;

   private String login;

   private String passcode;

   private String clientID;

   //this means login is valid. (stomp connection ok)
   private boolean valid;

   private boolean destroyed = false;

   private final long creationTime;

   private final Acceptor acceptorUsed;

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<CloseListener>();

   private final Object failLock = new Object();

   private boolean dataReceived;

   private final boolean enableMessageID;

   private StompVersions version;

   private VersionedStompFrameHandler frameHandler;

   //this means the version negotiation done.
   private boolean initialized;

   private FrameEventListener stompListener;

   private final Object sendLock = new Object();

   private final int minLargeMessageSize;

   public StompFrame decode(ActiveMQBuffer buffer) throws ActiveMQStompException {
      StompFrame frame = null;
      try {
         frame = frameHandler.decode(buffer);
      }
      catch (ActiveMQStompException e) {
         switch (e.getCode()) {
            case ActiveMQStompException.INVALID_EOL_V10:
               if (version != null)
                  throw e;
               frameHandler = new StompFrameHandlerV12(this);
               buffer.resetReaderIndex();
               frame = decode(buffer);
               break;
            case ActiveMQStompException.INVALID_COMMAND:
               frameHandler.onError(e);
               break;
            default:
               throw e;
         }
      }
      return frame;
   }

   public boolean hasBytes() {
      return frameHandler.hasBytes();
   }

   StompConnection(final Acceptor acceptorUsed,
                   final Connection transportConnection,
                   final StompProtocolManager manager) {
      this.transportConnection = transportConnection;

      this.manager = manager;

      this.frameHandler = new StompFrameHandlerV10(this);

      this.creationTime = System.currentTimeMillis();

      this.acceptorUsed = acceptorUsed;

      this.enableMessageID = ConfigurationHelper.getBooleanProperty(TransportConstants.STOMP_ENABLE_MESSAGE_ID, false, acceptorUsed.getConfiguration());
      this.minLargeMessageSize = ConfigurationHelper.getIntProperty(TransportConstants.STOMP_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, acceptorUsed.getConfiguration());
   }

   @Override
   public void addFailureListener(final FailureListener listener) {
      if (listener == null) {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   @Override
   public boolean removeFailureListener(final FailureListener listener) {
      if (listener == null) {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   @Override
   public void addCloseListener(final CloseListener listener) {
      if (listener == null) {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      closeListeners.add(listener);
   }

   @Override
   public boolean removeCloseListener(final CloseListener listener) {
      if (listener == null) {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      return closeListeners.remove(listener);
   }

   @Override
   public List<CloseListener> removeCloseListeners() {
      List<CloseListener> ret = new ArrayList<CloseListener>(closeListeners);

      closeListeners.clear();

      return ret;
   }

   @Override
   public List<FailureListener> removeFailureListeners() {
      List<FailureListener> ret = new ArrayList<FailureListener>(failureListeners);

      failureListeners.clear();

      return ret;
   }

   @Override
   public void setCloseListeners(List<CloseListener> listeners) {
      closeListeners.clear();

      closeListeners.addAll(listeners);
   }

   @Override
   public void setFailureListeners(final List<FailureListener> listeners) {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   protected synchronized void setDataReceived() {
      dataReceived = true;
   }

   public synchronized boolean checkDataReceived() {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   public void checkDestination(String destination) throws ActiveMQStompException {
      if (autoCreateQueueIfPossible(destination)) {
         return;
      }

      if (!manager.destinationExists(destination)) {
         throw BUNDLE.destinationNotExist(destination).setHandler(frameHandler);
      }
   }

   public boolean autoCreateQueueIfPossible(String queue) throws ActiveMQStompException {
      boolean autoCreated = false;

      if (queue.startsWith(ResourceNames.JMS_QUEUE) && manager.getServer().getAddressSettingsRepository().getMatch(queue).isAutoCreateJmsQueues() && manager.getServer().locateQueue(new SimpleString(queue)) == null) {
         SimpleString queueName = new SimpleString(queue);
         try {
            manager.getServer().createQueue(queueName, queueName, null, SimpleString.toSimpleString(this.getLogin()), true, false, true);
         }
         catch (Exception e) {
            throw new ActiveMQStompException(e.getMessage(), e).setHandler(frameHandler);
         }
         autoCreated = true;
      }

      return autoCreated;
   }

   @Override
   public ActiveMQBuffer createTransportBuffer(int size) {
      return ActiveMQBuffers.dynamicBuffer(size);
   }

   public void destroy() {
      synchronized (failLock) {
         if (destroyed) {
            return;
         }
      }

      destroyed = true;

      internalClose();

      synchronized (sendLock) {
         callClosingListeners();
      }
   }

   Acceptor getAcceptorUsed() {
      return acceptorUsed;
   }

   private void internalClose() {
      transportConnection.close();

      manager.cleanup(this);
   }

   public void fail(final ActiveMQException me) {
      synchronized (failLock) {
         if (destroyed) {
            return;
         }

         destroyed = true;
      }

      ActiveMQServerLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());
      // Then call the listeners
      callFailureListeners(me);

      callClosingListeners();

      internalClose();
   }

   public void fail(final ActiveMQException me, String scaleDownTargetNodeID) {
      fail(me);
   }

   public void flush() {
   }

   public List<FailureListener> getFailureListeners() {
      // we do not return the listeners otherwise the remoting service
      // would NOT destroy the connection.
      return Collections.emptyList();
   }

   public Object getID() {
      return transportConnection.getID();
   }

   public String getRemoteAddress() {
      return transportConnection.getRemoteAddress();
   }

   public long getCreationTime() {
      return creationTime;
   }

   public Connection getTransportConnection() {
      return transportConnection;
   }

   public boolean isClient() {
      return false;
   }

   public boolean isDestroyed() {
      return destroyed;
   }

   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
      manager.handleBuffer(this, buffer);
   }

   public String getLogin() {
      return login;
   }

   public String getPasscode() {
      return passcode;
   }

   public void setClientID(String clientID) {
      this.clientID = clientID;
   }

   public String getClientID() {
      return clientID;
   }

   public boolean isValid() {
      return valid;
   }

   public void setValid(boolean valid) {
      this.valid = valid;
   }

   private void callFailureListeners(final ActiveMQException me) {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone) {
         try {
            listener.connectionFailed(me, false);
         }
         catch (final Throwable t) {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            ActiveMQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   private void callClosingListeners() {
      final List<CloseListener> listenersClone = new ArrayList<CloseListener>(closeListeners);

      for (final CloseListener listener : listenersClone) {
         try {
            listener.connectionClosed();
         }
         catch (final Throwable t) {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            ActiveMQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   /*
    * accept-version value takes form of "v1,v2,v3..."
    * we need to return the highest supported version
    */
   public void negotiateVersion(StompFrame frame) throws ActiveMQStompException {
      String acceptVersion = frame.getHeader(Stomp.Headers.ACCEPT_VERSION);

      if (acceptVersion == null) {
         this.version = StompVersions.V1_0;
      }
      else {
         StringTokenizer tokenizer = new StringTokenizer(acceptVersion, ",");
         Set<String> requestVersions = new HashSet<String>(tokenizer.countTokens());
         while (tokenizer.hasMoreTokens()) {
            requestVersions.add(tokenizer.nextToken());
         }

         if (requestVersions.contains(StompVersions.V1_2.toString())) {
            this.version = StompVersions.V1_2;
         }
         else if (requestVersions.contains(StompVersions.V1_1.toString())) {
            this.version = StompVersions.V1_1;
         }
         else if (requestVersions.contains(StompVersions.V1_0.toString())) {
            this.version = StompVersions.V1_0;
         }
         else {
            //not a supported version!
            ActiveMQStompException error = BUNDLE.versionNotSupported(acceptVersion).setHandler(frameHandler);
            error.addHeader(Stomp.Headers.Error.VERSION, manager.getSupportedVersionsAsErrorVersion());
            error.addHeader(Stomp.Headers.CONTENT_TYPE, "text/plain");
            error.setBody("Supported protocol versions are " + manager.getSupportedVersionsAsString());
            error.setDisconnect(true);
            throw error;
         }
      }

      if (this.version != (StompVersions.V1_0)) {
         VersionedStompFrameHandler newHandler = VersionedStompFrameHandler.getHandler(this, this.version);
         newHandler.initDecoder(this.frameHandler);
         this.frameHandler = newHandler;
      }
      this.initialized = true;
   }

   //reject if the host doesn't match
   public void setHost(String host) throws ActiveMQStompException {
      if (host == null) {
         ActiveMQStompException error = BUNDLE.nullHostHeader().setHandler(frameHandler);
         error.setBody(BUNDLE.hostCannotBeNull());
         throw error;
      }

      String localHost = manager.getVirtualHostName();
      if (!host.equals(localHost)) {
         ActiveMQStompException error = BUNDLE.hostNotMatch().setHandler(frameHandler);
         error.setBody(BUNDLE.hostNotMatchDetails(host));
         throw error;
      }
   }

   public void handleFrame(StompFrame request) {
      StompFrame reply = null;

      if (stompListener != null) {
         stompListener.requestAccepted(request);
      }

      System.out.println("Frame::" + request);

      String cmd = request.getCommand();
      try {
         if (isDestroyed()) {
            throw BUNDLE.connectionDestroyed().setHandler(frameHandler);
         }
         if (!initialized) {
            if (!(Stomp.Commands.CONNECT.equals(cmd) || Stomp.Commands.STOMP.equals(cmd))) {
               throw BUNDLE.connectionNotEstablished().setHandler(frameHandler);
            }
            //decide version
            negotiateVersion(request);
         }

         reply = frameHandler.handleFrame(request);
      }
      catch (ActiveMQStompException e) {
         reply = e.getFrame();
      }

      if (reply != null) {
         sendFrame(reply);
      }

      if (Stomp.Commands.DISCONNECT.equals(cmd)) {
         this.disconnect(false);
      }
   }

   public void sendFrame(StompFrame frame) {
      manager.sendReply(this, frame);
   }

   public boolean validateUser(final String login1, final String passcode1) {
      this.valid = manager.validateUser(login1, passcode1);
      if (valid) {
         this.login = login1;
         this.passcode = passcode1;
      }
      return valid;
   }

   public ServerMessageImpl createServerMessage() {
      return manager.createServerMessage();
   }

   public StompSession getSession(String txID) throws ActiveMQStompException {
      StompSession session = null;
      try {
         if (txID == null) {
            session = manager.getSession(this);
         }
         else {
            session = manager.getTransactedSession(this, txID);
         }
      }
      catch (Exception e) {
         throw BUNDLE.errorGetSession(e).setHandler(frameHandler);
      }

      return session;
   }

   protected void validate() throws ActiveMQStompException {
      if (!this.valid) {
         throw BUNDLE.invalidConnection().setHandler(frameHandler);
      }
   }

   protected void sendServerMessage(ServerMessageImpl message, String txID) throws ActiveMQStompException {
      StompSession stompSession = getSession(txID);

      if (stompSession.isNoLocal()) {
         message.putStringProperty(CONNECTION_ID_PROP, getID().toString());
      }
      if (enableMessageID()) {
         message.putStringProperty("amqMessageId", "STOMP" + message.getMessageID());
      }
      try {
         if (minLargeMessageSize == -1 || (message.getBodyBuffer().writerIndex() < minLargeMessageSize)) {
            stompSession.sendInternal(message, false);
         }
         else {
            stompSession.sendInternalLarge(message, false);
         }
      }
      catch (Exception e) {
         throw BUNDLE.errorSendMessage(message, e).setHandler(frameHandler);
      }
   }

   @Override
   public void disconnect(final boolean criticalError) {
      disconnect(null, criticalError);
   }

   @Override
   public void disconnect(String scaleDownNodeID, final boolean criticalError) {
      destroy();
   }

   protected void beginTransaction(String txID) throws ActiveMQStompException {
      try {
         manager.beginTransaction(this, txID);
      }
      catch (ActiveMQStompException e) {
         throw e;
      }
      catch (Exception e) {
         throw BUNDLE.errorBeginTx(txID, e).setHandler(frameHandler);
      }
   }

   public void commitTransaction(String txID) throws ActiveMQStompException {
      try {
         manager.commitTransaction(this, txID);
      }
      catch (Exception e) {
         throw BUNDLE.errorCommitTx(txID, e).setHandler(frameHandler);
      }
   }

   public void abortTransaction(String txID) throws ActiveMQStompException {
      try {
         manager.abortTransaction(this, txID);
      }
      catch (ActiveMQStompException e) {
         throw e;
      }
      catch (Exception e) {
         throw BUNDLE.errorAbortTx(txID, e).setHandler(frameHandler);
      }
   }

   void subscribe(String destination,
                  String selector,
                  String ack,
                  String id,
                  String durableSubscriptionName,
                  boolean noLocal) throws ActiveMQStompException {
      autoCreateQueueIfPossible(destination);
      if (noLocal) {
         String noLocalFilter = CONNECTION_ID_PROP + " <> '" + getID().toString() + "'";
         if (selector == null) {
            selector = noLocalFilter;
         }
         else {
            selector += " AND " + noLocalFilter;
         }
      }

      if (ack == null) {
         ack = Stomp.Headers.Subscribe.AckModeValues.AUTO;
      }

      String subscriptionID = null;
      if (id != null) {
         subscriptionID = id;
      }
      else {
         if (destination == null) {
            throw BUNDLE.noDestination().setHandler(frameHandler);
         }
         subscriptionID = "subscription/" + destination;
      }

      try {
         manager.createSubscription(this, subscriptionID, durableSubscriptionName, destination, selector, ack, noLocal);
      }
      catch (ActiveMQStompException e) {
         throw e;
      }
      catch (Exception e) {
         throw BUNDLE.errorCreatSubscription(subscriptionID, e).setHandler(frameHandler);
      }
   }

   public void unsubscribe(String subscriptionID, String durableSubscriberName) throws ActiveMQStompException {
      try {
         manager.unsubscribe(this, subscriptionID, durableSubscriberName);
      }
      catch (ActiveMQStompException e) {
         throw e;
      }
      catch (Exception e) {
         throw BUNDLE.errorUnsubscrib(subscriptionID, e).setHandler(frameHandler);
      }
   }

   public void acknowledge(String messageID, String subscriptionID) throws ActiveMQStompException {
      try {
         manager.acknowledge(this, messageID, subscriptionID);
      }
      catch (ActiveMQStompException e) {
         throw e;
      }
      catch (Exception e) {
         throw BUNDLE.errorAck(messageID, e).setHandler(frameHandler);
      }
   }

   public String getVersion() {
      return String.valueOf(version);
   }

   public String getActiveMQServerName() {
      return SERVER_NAME;
   }

   public StompFrame createStompMessage(ServerMessage serverMessage,
                                        StompSubscription subscription,
                                        int deliveryCount) throws Exception {
      return frameHandler.createMessageFrame(serverMessage, subscription, deliveryCount);
   }

   public void addStompEventListener(FrameEventListener listener) {
      this.stompListener = listener;
   }

   //send a ping stomp frame
   public void ping(StompFrame pingFrame) {
      manager.sendReply(this, pingFrame);
   }

   public void physicalSend(StompFrame frame) throws Exception {
      ActiveMQBuffer buffer = frame.toActiveMQBuffer();
      synchronized (sendLock) {
         getTransportConnection().write(buffer, false, false);
      }

      if (stompListener != null) {
         stompListener.replySent(frame);
      }

   }

   public VersionedStompFrameHandler getFrameHandler() {
      return this.frameHandler;
   }

   public boolean enableMessageID() {
      return enableMessageID;
   }

   public int getMinLargeMessageSize() {
      return minLargeMessageSize;
   }

}
