package org.apache.zeppelin.notebook.repo.zeppelinhub;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

/**
 * ZeppelinHub token management class
 *
 */
public class ZeppelinHubTokenManager {
  private static final Logger LOG = LoggerFactory.getLogger(ZeppelinHubTokenManager.class);
  private static ZeppelinHubTokenManager instance = null;
  private static final String ZEPPELIN_CONF_PROP_NAME_TOKEN = "zeppelinhub.api.token";
  private static final String TOKEN_API_ENDPOINT = "api/v1/zeppelin-instances";
  private String token;
  
  private ZeppelinHubTokenManager() {
    ZeppelinConfiguration conf = ZeppelinConfiguration.create();
    this.setToken(conf.getString("ZEPPELINHUB_API_TOKEN", ZEPPELIN_CONF_PROP_NAME_TOKEN, ""));
  }

  private ZeppelinHubTokenManager(ZeppelinConfiguration conf) {
    this.setToken(conf.getString("ZEPPELINHUB_API_TOKEN", ZEPPELIN_CONF_PROP_NAME_TOKEN, ""));
  }

  public static ZeppelinHubTokenManager initialize(ZeppelinConfiguration conf) {
    if (instance == null) {
      instance = new ZeppelinHubTokenManager(conf);
    }
    return instance;
  }

  public static ZeppelinHubTokenManager getInstance() {
    if (instance == null) {
      instance = new ZeppelinHubTokenManager();
    }
    return instance;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String createToken(String zeppelinHubUrl, String cookie) {
    HttpClient httpClient = new HttpClient();
    Gson gson = new Gson();
    PostMethod post = new PostMethod(Joiner.on("/").join(zeppelinHubUrl, TOKEN_API_ENDPOINT));
    post.setRequestHeader("Cookie", "user_session=" + cookie);
    String requestBody = createTokenPayload();
    String responseBody = StringUtils.EMPTY;
    try {
      post.setRequestEntity(new StringRequestEntity(requestBody, "application/json", "UTF-8"));
      int statusCode = httpClient.executeMethod(post);
      if (statusCode != HttpStatus.SC_OK) {
        LOG.error("Cannot create token, HTTP status code is {} instead of 200 (OK)", statusCode);
        //throw exception?
        //post.releaseConnection();
      }
      responseBody = post.getResponseBodyAsString();
      post.releaseConnection();
    } catch (IOException e) {
      LOG.error("Cannot create token", e);
      //throw new AuthenticationException(e.getMessage());
    }

    String token = StringUtils.EMPTY;
    try {
      @SuppressWarnings("unchecked")
      Map<String, String> reply = gson.fromJson(responseBody, Map.class);
      if (reply.containsKey("token")) {
        token = reply.get("token");
        token = (token == null) ? StringUtils.EMPTY : token;
      }
    } catch (JsonParseException e) {
      LOG.error("Cannot deserialize ZeppelinHub response to Map instance", e);
      //throw new AuthenticationException("Cannot login to ZeppelinHub");
    }
    LOG.info("Created token from ZeppelinHub is {}", token);
    return token;
  }

  private String createTokenPayload() {
    StringBuilder sb = new StringBuilder("{\"name\":\"");
    String name = "khalid-zeppelin-1";
    String description = "username's Zeppelin";
    return sb.append(name).append("\", \"description\":\"").append(description).append("\"}")
        .toString();
  }
}
