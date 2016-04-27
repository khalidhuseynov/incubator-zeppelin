package org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.scheduler;

import org.apache.zeppelin.notebook.repo.zeppelinhub.ZeppelinhubRestApiHandler;
import org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.utils.ZeppelinhubUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routine that send PING event to zeppelinhub.
 *
 */
public class ZeppelinHubHeartbeat implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ZeppelinhubRestApiHandler.class);
  private Session ZeppelinhubSession;
  private final String token;
  
  public static ZeppelinHubHeartbeat newInstance(Session session, String token) {
    return new ZeppelinHubHeartbeat(session, token);
  }
  
  private ZeppelinHubHeartbeat(Session session, final String token) {
    ZeppelinhubSession = session;
    this.token = token;
  }
  
  @Override
  public void run() {
    LOG.debug("Sending PING to zeppelinhub");
    ZeppelinhubSession.getRemote().sendStringByFuture(ZeppelinhubUtils.PingMessage(token));
  }  
}
