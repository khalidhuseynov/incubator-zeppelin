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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * NoteRepo rest API endpoint.
 * 
 */
@Path("/notebook-repositories")
@Produces("application/json")
public class NoteRepoRestApi {

  private static final Logger LOG = LoggerFactory.getLogger(NoteRepoRestApi.class);
  
  Gson gson = new Gson();

  public NoteRepoRestApi() {
  }

  /**
   * List all notebook repository
   */
  @GET
  @ZeppelinApi
  public Response listSettings() {
    LOG.info("Getting list of NoteRepo");
    return new JsonResponse<>(Status.OK, "", "{}").build();
  }
  
  /**
   * Update a specific note repo.
   * 
   * @param message
   * @param settingId
   * @return
   */
  @PUT
  @Path("")
  @ZeppelinApi
  public Response updateSetting(String message, @PathParam("settingId") String settingId) {
    return new JsonResponse<>(Status.OK, "", "").build();
  }
  
}
