package org.apache.zeppelin.notebook.repo.zeppelinhub.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO(xxx): Add description
 * 
 */
public class Client {
  private Logger LOG = LoggerFactory.getLogger(Client.class);
  private final ZeppelinhubClient zeppelinhubClient;
  public ZeppelinClient zeppelinClient;
  public String token;

  public Client(String zeppelinUri, String zeppelinhubUri, String token) {
    LOG.debug("Init Client");
    this.token = token;
    zeppelinhubClient = ZeppelinhubClient.newInstance(zeppelinhubUri, token);
    zeppelinClient = ZeppelinClient.newInstance(zeppelinUri, this);
  }
  
  public void start() {
    if (zeppelinhubClient != null) {
      zeppelinhubClient.start();
      if (zeppelinClient != null) {
        zeppelinClient.start();
      }
    }
  }
  
  public void stop() {
    if (zeppelinhubClient != null) {
      zeppelinhubClient.stop();
    }
    if (zeppelinClient != null) {
      zeppelinClient.stop();
    }
  }

}
