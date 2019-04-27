/*
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.hopsworks.common.provenance;

import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;

import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.logging.Level;

public class MLAssetListQueryParams {
  public final Integer projectId;
  public final String assetName;
  public final String likeAssetName;
  public final String userName;
  public final String likeUserName;
  public final Long createdBeforeTimestamp;
  public final Long createdAfterTimestamp;
  public final boolean withAppState;
  public final Optional<Provenance.AppState> currentState;
  public final Map<String, String> xattrs;

  private MLAssetListQueryParams(Integer projectId, 
    String assetName, String likeAssetName,
    String userName, String likeUserName, 
    Long createdBeforeTimestamp, Long createdAfterTimestamp,
    boolean withAppState, Provenance.AppState currentState,
    Map<String, String> xattrs) {
    this.projectId = projectId;
    this.assetName = assetName;
    this.likeAssetName = likeAssetName;
    this.userName = userName;
    this.likeUserName = likeAssetName;
    this.createdBeforeTimestamp = createdBeforeTimestamp;
    this.createdAfterTimestamp = createdAfterTimestamp;
    this.withAppState = withAppState;
    this.currentState = Optional.ofNullable(currentState);
    this.xattrs = xattrs;
  }

  public static MLAssetListQueryParams instance(Integer projectId, 
    String assetName, String likeAssetName,
    String userName, String likeUserName, 
    Long createdBeforeTimestamp, Long createdAfterTimestamp,
    boolean withAppState, Provenance.AppState currentState,
    Map<String, String> xattrs) throws GenericException {
    if (assetName != null && likeAssetName != null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "provenance query - set only one - either like or exact - assetName");
    }
    if (projectId == null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "project provenance query - always confined to own project");
    }
    if (userName != null && likeUserName != null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "provenance query - set only one - either like or exact - userName");
    }
    if (currentState != null && !withAppState) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "provenance query - in order to filter by current state - withAppState has to be enabled");
    }
    return new MLAssetListQueryParams(projectId, assetName, likeAssetName, userName, likeUserName,
      createdBeforeTimestamp, createdAfterTimestamp, withAppState, currentState, xattrs);
  }
  
  public static MLAssetListQueryParams projectMLAssets(Integer projectId, boolean withAppState) 
    throws GenericException {
    return instance(projectId, null, null, null, null,
      null, null, withAppState, null, null);
  }
  
  public static Map<String, String> getXAttrsMap(String xattrs) throws GenericException {
    Map<String, String> result = new TreeMap<String, String>();
    if (xattrs == null || xattrs.isEmpty()) {
      return result;
    }
    String[] params = xattrs.split(",");
    
    for (String p : params) {
      String[] aux = p.split(":");
      if(aux.length != 2 || aux[0].isEmpty()) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
          "malformed xattrs:" + xattrs);
      }
      String keyParts[] = aux[0].split("\\.");
      StringJoiner keyj = new StringJoiner(".");
      if(keyParts.length == 1) {
        keyj.add(keyParts[0]).add("raw");
      } else {
        keyj.add(keyParts[0]).add("value");
        for(int i = 1; i < keyParts.length; i++) keyj.add(keyParts[i]);
      }
      result.put(keyj.toString(), aux[1]);
    }
    return result;
  }
}
