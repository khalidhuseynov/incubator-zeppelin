package org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.protocol;

import java.util.Map;

import org.apache.zeppelin.notebook.socket.Message.OP;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

/**
 * Zeppelinhub message class.
 *
 */
public class ZeppelinhubMessage {
  private static final Gson gson = new Gson();

  public OP op;
  public Object data;
  public Map<String, String> meta = Maps.newHashMap();
  
  private ZeppelinhubMessage(OP op, Object data, Map<String, String> meta) {
    this.op = op;
    this.data = data;
    this.meta = meta;
  }
  
  public static ZeppelinhubMessage newMessage(OP op, Object data, Map<String, String> meta) {
    return new ZeppelinhubMessage(op, data, meta);
  }
  
  public String serialize() {
    return gson.toJson(this, ZeppelinhubMessage.class);
  }
  
  public static ZeppelinhubMessage deserialize(String zeppelinhubMessage) {
    return gson.fromJson(zeppelinhubMessage, ZeppelinhubMessage.class);
  }
  
}
