package org.apache.zeppelin.notebook.repo.zeppelinhub.websocket;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.protocol.ZeppelinhubMessage;
import org.apache.zeppelin.notebook.socket.Message;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Zeppelin websocket client.
 *
 */
public class ZeppelinClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZeppelinClient.class);
  private final URI zeppelinWebsocketUrl;
  private final String zeppelinhubToken;
  private final WebSocketClient wsClient;
  private static Gson gson;
  private ConcurrentHashMap<String, Session> zeppelinConnectionMap;

  public static ZeppelinClient newInstance(String url, String token) {
    return new ZeppelinClient(url, token);
  }

  private ZeppelinClient(String zeppelinUri, String token) {
    zeppelinWebsocketUrl = URI.create(zeppelinUri);
    zeppelinhubToken = token;
    wsClient = new WebSocketClient();
    gson = new Gson();
  }

  public void start() {
    try {
      if (wsClient != null) {
        wsClient.start();
      } else {
        LOG.warn("Cannot start zeppelin websocket client - isn't initialized");
      }
    } catch (Exception e) {
      LOG.error("Cannot start Zeppelin websocket client", e);
    }
  }

  public void stop() {
    try {
      if (wsClient != null) {
        wsClient.stop();
      } else {
        LOG.warn("Cannot stop zeppelin websocket client - isn't initialized");
      }
    } catch (Exception e) {
      LOG.error("Cannot stop Zeppelin websocket client", e);
    }
  }

  public String serialize(Message zeppelinMsg) {
    // TODO(khalid): handle authentication
    String msg = gson.toJson(zeppelinMsg);
    return msg;
  }

  public Message deserialize(String zeppelinMessage) {
    if (StringUtils.isBlank(zeppelinMessage)) {
      return null;
    }
    Message msg;
    try {
      msg = gson.fromJson(zeppelinMessage, Message.class);
    } catch (JsonSyntaxException ex) {
      LOG.error("Cannot deserialize zeppelin message", ex);
      msg = null;
    }
    return msg;
  }

  public void send(Message msg, String noteId) {
    Session noteSession = getZeppelinConnection(noteId);
    if (!isSessionOpen(noteSession)) {
      LOG.error("Cannot open websocket connection to Zeppelin note {}", noteId);
      return;
    }
    noteSession.getRemote().sendStringByFuture(serialize(msg));
  }

  private boolean isSessionOpen(Session session) {
    return (session != null) && (session.isOpen());
  }

  /* per notebook based ws connection, returns null if can't connect */
  public Session getZeppelinConnection(String noteId) {
    if (StringUtils.isBlank(noteId)) {
      LOG.warn("Cannot return websocket connection for blank noteId");
      return null;
    }
    // return existing connection
    if (zeppelinConnectionMap.containsKey(noteId)) {
      LOG.info("Connection for {} exists in map", noteId);
      return zeppelinConnectionMap.get(noteId);
    }

    // create connection
    ClientUpgradeRequest request = new ClientUpgradeRequest();
    ZeppelinWebsocket socket = new ZeppelinWebsocket(noteId);
    Future<Session> future = null;
    Session session = null;
    try {
      future = wsClient.connect(socket, zeppelinWebsocketUrl, request);
      session = future.get();
    } catch (IOException | InterruptedException | ExecutionException e) {
      LOG.error("Couldn't establish websocket connection to Zeppelin ", e);
      return null;
    }

    if (zeppelinConnectionMap.containsKey(noteId)) {
      session.close();
      session = zeppelinConnectionMap.get(noteId);
    } else {
      zeppelinConnectionMap.put(noteId, session);
    }
    //TODO(khalid): clean log later
    LOG.info("Create Zeppelin websocket connection {} {}", zeppelinWebsocketUrl, noteId);
    return session;
  }

  /**
   * Close and remove ZeppelinConnection
   */
  public void removeZeppelinConnection(String noteId) {
    if (zeppelinConnectionMap.containsKey(noteId)) {
      Session conn = zeppelinConnectionMap.get(noteId);
      if (conn.isOpen()) {
        conn.close();
      }
      zeppelinConnectionMap.remove(noteId);
    }
    // TODO(khalid): clean log later
    LOG.info("Removed Zeppelin ws connection for the following note {}", noteId);
  }

  private void sendToHub(ZeppelinWebsocket socket, String msgFromZeppelin) {
    Map<String, String> meta = new HashMap<String, String>();
    meta.put("token", zeppelinhubToken);
    meta.put("noteId", socket.noteId);
    Message zeppelinMsg = deserialize(msgFromZeppelin);
    if (zeppelinMsg == null) {
      return;
    }
    ZeppelinhubMessage hubMsg = ZeppelinhubMessage.newMessage(zeppelinMsg, meta);
    Client client = Client.getInstance();
    if (client == null) {
      LOG.warn("Client isn't initialized yet");
      return;
    }
    Client.getInstance().relayToHub(hubMsg.serialize());
  }

  /**
   * Zeppelin websocket listener class.
   *
   */
  public class ZeppelinWebsocket implements WebSocketListener {
    public Session connection;
    public String noteId;

    public ZeppelinWebsocket(String noteId) {
      this.noteId = noteId;
    }

    @Override
    public void onWebSocketBinary(byte[] arg0, int arg1, int arg2) {

    }

    @Override
    public void onWebSocketClose(int code, String message) {
      LOG.info("Zeppelin connection closed with code: {}, message: {}", code, message);
      // parentClient.removeConnMap(noteId);
    }

    @Override
    public void onWebSocketConnect(Session session) {
      LOG.info("Zeppelin connection opened");
      this.connection = session;
    }

    @Override
    public void onWebSocketError(Throwable e) {
      LOG.warn("Zeppelin socket connection error ", e);
    }

    @Override
    public void onWebSocketText(String data) {
      LOG.debug("Zeppelin client received Message: " + data);
      // propagate to ZeppelinHub
      try {
        sendToHub(this, data);
      } catch (Exception e) {
        LOG.error("Failed to send message to ZeppelinHub: ", e);
      }
    }

  }

}
