/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.notebook.repo.NotebookRepoSync;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

/**
 * NoteRepo rest API endpoint.
 * 
 */
@Path("/notebook-repositories")
@Produces("application/json")
public class NotebookRepoRestApi {

  private static final Logger LOG = LoggerFactory.getLogger(NotebookRepoRestApi.class);
  Gson gson = new Gson();
  private NotebookRepoSync noteRepos;

  public NotebookRepoRestApi() {}
  
  public NotebookRepoRestApi(NotebookRepoSync noteRepos) {
    this.noteRepos = noteRepos;
  }

  /**
   * List all notebook repository
   */
  @GET
  @ZeppelinApi
  public Response listSettings() {
    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    LOG.info("Getting list of NoteRepo for user {}", subject.getUser());
    return new JsonResponse<>(Status.OK, "", noteRepos.getNotebookRepos(subject)).build();
  }
  
  /**
   * Update a specific note repo.
   * 
   * @param message
   * @param settingId
   * @return
   */
  @GET
  @Path("{repoName}")
  @ZeppelinApi
  public Response updateSetting(String message, @PathParam("repoName") String repoName) {
    return new JsonResponse<>(Status.OK, "", Maps.newHashMap()).build();
  }
  
}
