package org.apache.zeppelin.notebook.repo.zeppelinhub;

import java.io.IOException;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZeppelinhubRestApiHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ZeppelinhubRestApiHandler.class);
  public static final String ZEPPELIN_TOKEN_HEADER = "X-Zeppelin-Token";
  private static final String DEFAULT_API_PATH = "/api/v1/zeppelin";
  private static boolean PROXY_ON = false;
  private static String PROXY_HOST;
  private static int PROXY_PORT;

  private final HttpClient client;
  private final String zepelinhubUrl;
  private final String token;

  public static ZeppelinhubRestApiHandler newInstance(String zeppelinhubUrl,
      String token) {
    return new ZeppelinhubRestApiHandler(zeppelinhubUrl, token);
  }

  private ZeppelinhubRestApiHandler(String zeppelinhubUrl, String token) {
    this.zepelinhubUrl = zeppelinhubUrl + DEFAULT_API_PATH + "/";
    this.token = token;

    //TODO(khalid):to make proxy conf consistent with Zeppelin confs
    readProxyConf();
    client = getHttpClient();
  }

  private void readProxyConf() {
    //try reading http_proxy
    String proxyHostString = StringUtils.isBlank(System.getenv("http_proxy"))?
        System.getenv("HTTP_PROXY"):System.getenv("http_proxy");
    if (StringUtils.isBlank(proxyHostString)) {
      //try https_proxy if no http_proxy
      proxyHostString = StringUtils.isBlank(System.getenv("https_proxy"))?
          System.getenv("HTTPS_PROXY"):System.getenv("https_proxy");
    }

    if (StringUtils.isBlank(proxyHostString)) {
      PROXY_ON = false;
    } else {
      // host format - http://domain:port/
      String[] parts = proxyHostString.replaceAll("/", "").split(":");
      if (parts.length !=3) {
        LOG.warn("Proxy host format is incorrect {}, e.g. http://domain:port/", proxyHostString);
        PROXY_ON = false;
        return;
      }
      PROXY_HOST = parts[1];
      PROXY_PORT = Integer.parseInt(parts[2]);
      LOG.info("Proxy protocol: {}, domain: {}, port: {}", parts[0], parts[1], parts[2]);
      PROXY_ON = true;
    }
  }

  private HttpClient getHttpClient() {
    HttpClient httpClient;
    MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
    httpClient = new HttpClient(connectionManager);

    if (PROXY_ON) {
      HostConfiguration config = httpClient.getHostConfiguration();
      config.setProxy(PROXY_HOST, PROXY_PORT);
      httpClient.setHostConfiguration(config);
    }
    return httpClient;
  }

  public String get(String argument) throws IOException {
    GetMethod get = getMethod(argument);
    int code = executeMethod(get);
    if (code == HttpStatus.SC_OK) {
      String content = new String(get.getResponseBody(), "UTF-8");
      get.releaseConnection();
      return content;
    } else {
      LOG.error(
          "ZeppelinhubRestApiHandler failed to perform get#{} HttpStatus {}",
          argument, code);
      get.releaseConnection();
      throw new IOException("Zeppelinhub failed request get [" + argument
          + "] HttpStatus " + code);
    }
  }

  public boolean put(String jsonNote) throws IOException {
    PutMethod put = putMethod("");
    put.setRequestEntity(new StringRequestEntity(jsonNote, "application/json",
        "UTF-8"));
    int code = executeMethod(put);
    put.releaseConnection();
    return code == HttpStatus.SC_OK;
  }

  public boolean del(String argument) throws IOException {
    DeleteMethod del = delMethod(argument);
    int code = executeMethod(del);
    del.releaseConnection();
    if (code == HttpStatus.SC_OK) {
      LOG.info("Zeppelinhub removed note {}", argument);
      return true;
    } else {
      return false;
    }
  }

  private GetMethod getMethod(String path) {
    GetMethod get = new GetMethod(zepelinhubUrl + path);
    get.setRequestHeader(ZEPPELIN_TOKEN_HEADER, token);
    return get;
  }

  private PutMethod putMethod(String path) {
    PutMethod put = new PutMethod(zepelinhubUrl + path);
    put.setRequestHeader(ZEPPELIN_TOKEN_HEADER, token);
    return put;
  }

  private DeleteMethod delMethod(String path) {
    DeleteMethod del = new DeleteMethod(zepelinhubUrl + path);
    del.setRequestHeader(ZEPPELIN_TOKEN_HEADER, token);
    return del;
  }

  public void close() {

  }
}
