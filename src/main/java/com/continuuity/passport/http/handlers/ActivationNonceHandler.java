/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;


import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.service.DataManagementService;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Defines end points for Account activation based nonce
 */
@Path("/passport/v1")
@Singleton
public class ActivationNonceHandler extends PassportHandler {

  private final DataManagementService dataManagementService;

  @Inject
  public ActivationNonceHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }

  @Path("generateActivationKey/{id}")
  @GET
  @Produces("application/json")
  public Response getActivationNonce(@PathParam("id") int id) {
    requestReceived();
    try {
      int nonce = dataManagementService.getActivationNonce(id);
      if (nonce != -1) {
        requestSuccess();
        return Response.ok(Utils.getNonceJson(nonce)).build();
      } else {
        requestFailed();
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getNonceJson("Couldn't generate nonce", id))
          .build();
      }
    } catch (RuntimeException e) {
      requestFailed();
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getNonceJson("Couldn't generate nonce", id))
        .build();
    }
  }

  @Path("getActivationId/{id}")
  @GET
  @Produces("application/json")
  public Response getActivationId(@PathParam("id") int id) {
    requestReceived();
    try {
      int nonce = dataManagementService.getActivationId(id);
      if (nonce != -1) {
        requestSuccess();
        return Response.ok(Utils.getNonceJson(nonce)).build();
      } else {
        requestFailed();
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getNonceJson("ID not found for nonce", id))
          .build();
      }
    } catch (StaleNonceException e) {
      requestFailed();
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getNonceJson("ID not found for nonce", id))
        .build();
    }
  }


}
