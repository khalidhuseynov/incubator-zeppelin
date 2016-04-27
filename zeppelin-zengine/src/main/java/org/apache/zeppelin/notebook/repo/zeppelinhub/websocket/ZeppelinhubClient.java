package org.apache.zeppelin.notebook.repo.zeppelinhub.websocket;


import java.net.HttpCookie;
import java.net.URI;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.zeppelin.notebook.repo.zeppelinhub.ZeppelinHubRepo;
import org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.scheduler.SchedulerService;
import org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.scheduler.ZeppelinHubHeartbeat;
import org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.utils.ZeppelinhubUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Zeppelinhub websocket client.
 * 
 * This class connect to Zeppelinhub and send/recieve event.
 *
 */
public class ZeppelinhubClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZeppelinhubClient.class);

  private final WebSocketClient client;
  private final URI zeppelinhubWebsocketUrl;
  private final ClientUpgradeRequest conectionRequest;
  
  // Websocket listenner to zeppelinhub.
  private final ZeppelinhubWebsocket socket;
  private final String zeppelinhubToken;
  
  private static final int MB = 1048576;
  private static final int MAXIMUN_TEXT_SIZE = 64 * MB;
  private static final long CONNECTION_IDLE_TIME = TimeUnit.SECONDS.toMillis(30);
  
  private SchedulerService schedulerService;
  
  private ZeppelinhubClient(String url, String token) {
    zeppelinhubWebsocketUrl = URI.create(url);
    client = createNewWebsocketClient();
    conectionRequest = setConnectionrequest(token);
    socket = new ZeppelinhubWebsocket();
    zeppelinhubToken = token;
    schedulerService = SchedulerService.create(10);
  }
  
  private ClientUpgradeRequest setConnectionrequest(String token) {
    ClientUpgradeRequest request = new ClientUpgradeRequest();
    request.setCookies(Lists.newArrayList(new HttpCookie(ZeppelinHubRepo.TOKEN_HEADER, token)));
    return request;
  }
  
  private WebSocketClient createNewWebsocketClient() {
    WebSocketClient client = new WebSocketClient();
    client.setMaxTextMessageBufferSize(MAXIMUN_TEXT_SIZE);
    client.setMaxIdleTimeout(CONNECTION_IDLE_TIME);
    return client;
  }
  
  private void addRoutines(Session session) {
    schedulerService.add(ZeppelinHubHeartbeat.newInstance(session, zeppelinhubToken), 10, 23);
  }
  
  public static ZeppelinhubClient newInstance(String url, String token) {
    return new ZeppelinhubClient(url, token);
  }
  
  public void start() {
    try {
      client.start();
      Future<Session> future = client.connect(socket, zeppelinhubWebsocketUrl, conectionRequest);
      Session session = future.get();
      addRoutines(session);
    } catch (Exception e) {
      LOG.error("Cannot connect to zeppelinhub via websocket", e);
    }
  }
  
  public void stop() {
    LOG.info("Stopping Zeppelinhub websocket client");
    try {
      socket.stop();
      client.stop();
    } catch (Exception e) {
      LOG.error("Cannot stop zeppelinhub websocket client", e);
    }
  }
  
  /**
   * Zeppelinhub websocket handler.
   */
  public class ZeppelinhubWebsocket implements WebSocketListener {
    private Logger LOG = LoggerFactory.getLogger(ZeppelinhubWebsocket.class);
    private Session zeppelinHubSession;

    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {}

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
      LOG.info("Closing websocket connection [{}] : {}", statusCode, reason);
      this.zeppelinHubSession = null;
    }

    @Override
    public void onWebSocketConnect(Session session) {
      LOG.info("Opening a new session to Zeppelinhub {}", session.hashCode());
      this.zeppelinHubSession = session;
      send(ZeppelinhubUtils.liveMessage(zeppelinhubToken));
    }

    @Override
    public void onWebSocketError(Throwable cause) {
      LOG.info("Got error", cause);
    }

    @Override
    public void onWebSocketText(String message) {
      LOG.info("Got msg {}", message);
      if (isSessionOpen()) {
        // do something.
      }
    }

    private boolean isSessionOpen() {
      return ((zeppelinHubSession != null) && (zeppelinHubSession.isOpen())) ? true : false;
    }
    
    private void send(String msg) {
      if (isSessionOpen()) {
        zeppelinHubSession.getRemote().sendStringByFuture(msg);
      }
    }
    
    private void stop() {
      send(ZeppelinhubUtils.DeadMessage(zeppelinhubToken));
      if (isSessionOpen()) {
        zeppelinHubSession.close();
      }
    }
    
  }
}
