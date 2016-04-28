package org.apache.zeppelin.notebook.repo.zeppelinhub.websocket;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.protocol.ZeppelinHubOp;
import org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.protocol.ZeppelinhubMessage;
import org.apache.zeppelin.notebook.repo.zeppelinhub.websocket.utils.ZeppelinhubUtils;
import org.apache.zeppelin.notebook.socket.Message;
import org.apache.zeppelin.notebook.socket.Message.OP;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;


/**
 * TODO(xxx): Add description
 * 
 */
public class Client {
  private static final Logger LOG = LoggerFactory.getLogger(Client.class);
  public final ZeppelinhubClient zeppelinhubClient;
  private static ZeppelinClient zeppelinClient;
  public String token;
  private static Gson gson;

  public Client(String zeppelinUri, String zeppelinhubUri, String token) {
    LOG.debug("Init Client");
    this.token = token;
    gson = new Gson();
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

  public static void handleMsgFromZeppelinHub(String msg, Session incoming, Session outgoing) {
    ZeppelinhubMessage hubMsg = ZeppelinhubMessage.deserialize(msg);
    if (hubMsg.equals(ZeppelinhubMessage.EMPTY)) {
      LOG.error("Cannot handle ZeppelinHub message is empty");
      return;
    }
    String op = StringUtils.EMPTY;
    if (hubMsg.op instanceof String) {
      op = (String) hubMsg.op;
    } else {
      LOG.error("Message OP from ZeppelinHub isn't string {}", hubMsg.op);
      return;
    }
    if (ZeppelinhubUtils.isHubOp(op)) {
      handleHubOpMsg(ZeppelinhubUtils.stringToHubOp(op), hubMsg);
    } else if (ZeppelinhubUtils.isZeppelinOp(op)) {
      forwardToZeppelin(ZeppelinhubUtils.stringToZeppelinOp(op), hubMsg);
    }
  }

  @SuppressWarnings("unchecked")
  private static void forwardToZeppelin(Message.OP op, ZeppelinhubMessage hubMsg) {
    Message zeppelinMsg = new Message(op);
    if (!(hubMsg.data instanceof Map)) {
      LOG.error("Data field of message from ZeppelinHub isn't in correct Map format");
      return;
    }
    zeppelinMsg.data = (Map<String, Object>) hubMsg.data;
    Session conn = zeppelinClient.getZeppelinConnection(hubMsg.meta.get("noteId"));
    if (conn != null) {
      conn.getRemote().sendStringByFuture(serialize(zeppelinMsg));
    }
  }

  private static void handleHubOpMsg(ZeppelinHubOp op, ZeppelinhubMessage msg) {
    if (op == null || msg.equals(ZeppelinhubMessage.EMPTY)) {
      LOG.error("Cannot handle empty op or msg");
      return;
    }
    switch (op) {
    case RUN_NOTEBOOK:
      runAllParagraph(msg.meta.get("noteId"), msg);
      break;
    case PONG:
      // do nothing
      break;
    default:
      LOG.warn("Received {} from ZeppelinHub, not handled", op);
      break;
    }
  }

  private static void runAllParagraph(String noteId, ZeppelinhubMessage hubMsg) {
    LOG.info("Running paragraph with noteId {}", noteId);
    try {
      JSONObject data = new JSONObject(hubMsg);
      if (data.equals(JSONObject.NULL) || !(data.get("data") instanceof JSONArray)) {
        LOG.error("Wrong \"data\" format for RUN_NOTEBOOK");
        return;
      }
      Session conn = zeppelinClient.getZeppelinConnection(noteId);
      if (conn == null) {
        LOG.warn("Couldn't acquire websocket connection for note {}", noteId);
        return;
      }
      Message zeppelinMsg = new Message(OP.RUN_PARAGRAPH);

      JSONArray paragraphs = data.getJSONArray("data");
      for (int i = 0; i < paragraphs.length(); i++) {
        if (!(paragraphs.get(i) instanceof JSONObject)) {
          LOG.warn("Wrong \"paragraph\" format for RUN_NOTEBOOK");
          continue;
        }
        zeppelinMsg.data = gson.fromJson(paragraphs.getString(i), new TypeToken<Map<String, Object>>(){}.getType());
        //conn.sendMessage(parentClient.serialize(zeppelinMsg));
        conn.getRemote().sendStringByFuture(serialize(zeppelinMsg));
        LOG.info("\nSending RUN_PARAGRAPH message to Zeppelin ");
      }
    } catch (JSONException e) {
        LOG.error("Failed to parse RUN_NOTEBOOK message from ZeppelinHub ", e);
    }
  }

  static String serialize(Message zeppelinMsg) {
    // TODO(khalid): handle authentication
    String msg = gson.toJson(zeppelinMsg);
    LOG.debug("Serialized Zeppelin message is {}", msg);
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
}
