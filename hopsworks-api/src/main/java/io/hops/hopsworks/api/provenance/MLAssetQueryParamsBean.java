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

import io.hops.hopsworks.common.provenance.MLAssetQueryParams;
import io.hops.hopsworks.exceptions.GenericException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;

public class MLAssetQueryParamsBean {
  @QueryParam("inodeId")
  private Long inodeId;
  
  @QueryParam("mlId")
  private String mlId;
  
  @QueryParam("withAppState") 
  private boolean withAppState;
  
  public MLAssetQueryParamsBean(@QueryParam("inodeId") Long inodeId,
    @QueryParam("mlId") String mlId,
    @QueryParam("withAppState") @DefaultValue("false") boolean withAppState) {
    this.inodeId = inodeId;
    this.mlId = mlId;
    this.withAppState = withAppState;
  }
  
  public MLAssetQueryParamsBean() {
  }

  public Long getInodeId() {
    return inodeId;
  }

  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }

  public String getMlId() {
    return mlId;
  }

  public void setMlId(String mlId) {
    this.mlId = mlId;
  }

  public boolean isWithAppState() {
    return withAppState;
  }

  public void setWithAppState(boolean withAppState) {
    this.withAppState = withAppState;
  }
  
  public MLAssetQueryParams params(Integer projectId) throws GenericException {
    return MLAssetQueryParams.instance(projectId, inodeId, mlId, withAppState);
  }

  @Override
  public String toString() {
    return "MLAssetQueryParamsBean{" 
      + (inodeId == null ? "" : " inodeId=" + inodeId)
      + (mlId == null ? "" : " mlId=" + mlId)
      + " withAppState=" + withAppState 
      + '}';
  }
}
