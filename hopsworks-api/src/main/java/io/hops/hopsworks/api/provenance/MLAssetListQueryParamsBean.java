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
package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.common.provenance.MLAssetListQueryParams;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.exceptions.GenericException;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;
import java.util.Map;

public class MLAssetListQueryParamsBean {
  @QueryParam("assetName")
  private String assetName;
  
  @QueryParam("likeAssetName")
  private String likeAssetName;
   
  @QueryParam("userName")
  private String userName;
  
  @QueryParam("likeUserName")
  private String likeUserName;
  
  @QueryParam("createdBefore")
  private Long createdBeforeTimestamp;
  
  @QueryParam("createdAfter")
  private Long createdAfterTimestamp;

  @DefaultValue("false") 
  @QueryParam("withAppState") 
  private boolean withAppState;
  
  @QueryParam("currentState")
  private Provenance.AppState currentState;

  @QueryParam("xattrs")
  @ApiParam(value = "ex. key1:val1,key2:val2")
  private String xattrs;
  
  public MLAssetListQueryParamsBean(@QueryParam("assetName") String assetName, 
    @QueryParam("likeAssetName") String likeAssetName, 
    @QueryParam("userName") String userName,
    @QueryParam("likeUserName") String likeUserName,
    @QueryParam("createdBefore") long createdBeforeTimestamp, 
    @QueryParam("createdAfter") long createdAfterTimestamp, 
    @QueryParam("withAppState") @DefaultValue("false") boolean withAppState,
    @QueryParam("currentState") Provenance.AppState currentState,
    @QueryParam("xattrs") String xattrs) {
    this.assetName = assetName;
    this.likeAssetName = likeAssetName;
    this.userName = userName;
    this.likeUserName = likeUserName;
    this.createdBeforeTimestamp = createdBeforeTimestamp;
    this.createdAfterTimestamp = createdAfterTimestamp;
    this.withAppState = withAppState;
    this.currentState = currentState;
    this.xattrs = xattrs;
  }
  
  public MLAssetListQueryParamsBean() {
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public Long getCreatedBeforeTimestamp() {
    return createdBeforeTimestamp;
  }

  public void setCreatedBeforeTimestamp(Long createdBeforeTimestamp) {
    this.createdBeforeTimestamp = createdBeforeTimestamp;
  }

  public Long getCreatedAfterTimestamp() {
    return createdAfterTimestamp;
  }

  public void setCreatedAfterTimestamp(Long createdAfterTimestamp) {
    this.createdAfterTimestamp = createdAfterTimestamp;
  }

  public Provenance.AppState getCurrentState() {
    return currentState;
  }

  public void setCurrentState(Provenance.AppState currentState) {
    this.currentState = currentState;
  }

  public String getAssetName() {
    return assetName;
  }

  public void setAssetName(String assetName) {
    this.assetName = assetName;
  }

  public String getLikeAssetName() {
    return likeAssetName;
  }

  public void setLikeAssetName(String likeAssetName) {
    this.likeAssetName = likeAssetName;
  }

  public String getLikeUserName() {
    return likeUserName;
  }

  public void setLikeUserName(String likeUserName) {
    this.likeUserName = likeUserName;
  }

  public boolean isWithAppState() {
    return withAppState;
  }

  public void setWithAppState(boolean withAppState) {
    this.withAppState = withAppState;
  }

  public String getXattrs() {
    return xattrs;
  }

  public void setXattrs(String xattrs) throws GenericException {
    this.xattrs = xattrs;
  }

  @Override
  public String toString() {
    return "MLAssetListQueryParamsBean{" 
      + (assetName == null ? "" : " assetName=" + assetName)
      + (likeAssetName == null ? "" : " likeAssetName=" + likeAssetName)
      + (userName == null ? "" : " userName=" + userName)
      + (likeUserName == null ? "" : " likeUserName=" + likeUserName) 
      + (createdBeforeTimestamp == null ? "" : " createdBeforeTimestamp=" + createdBeforeTimestamp)
      + (createdAfterTimestamp == null ? "" :" createdAfterTimestamp=" + createdAfterTimestamp)
      + " withAppState=" + withAppState 
      + (currentState == null ? "" : " currentState=" + currentState)
      + (xattrs == null ? "" : " xattrs=" + xattrs)
      + '}';
  }

  public MLAssetListQueryParams params(Integer projectId) throws GenericException {
    Map<String, String> xattrsMap = null;
    if(xattrs != null) {
      xattrsMap = MLAssetListQueryParams.getXAttrsMap(xattrs);
    }
    return MLAssetListQueryParams.instance(projectId, assetName, likeAssetName,
      userName, likeUserName, createdBeforeTimestamp, createdAfterTimestamp, withAppState, currentState, xattrsMap);
  }
}
