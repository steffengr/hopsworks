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
import java.util.logging.Level;

public class MLAssetQueryParams {

  public final Integer projectId;
  public final Long inodeId;
  public final String mlId;
  public final boolean withAppState;

  private MLAssetQueryParams(Integer projectId, Long inodeId, String mlId, boolean withAppState) {
    this.projectId = projectId;
    this.inodeId = inodeId;
    this.mlId = mlId;
    this.withAppState = withAppState;
  }
  
  @Override
  public String toString() {
    return "MLAssetQueryParams{" 
      + " projectId=" + projectId
      + (inodeId == null ? "" : " inodeId=" + inodeId)
      + (mlId == null ? "" : " mlId=" + mlId)
      + " withAppState=" + withAppState 
      + '}';
  }

  public static MLAssetQueryParams instance(Integer projectId, Long inodeId, String mlId, boolean withAppState)
    throws GenericException {
    if (projectId == null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "project provenance query - always confined to own project");
    }
    if ((inodeId == null && mlId == null) || (inodeId != null && mlId != null)) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "provenance query - you need to provide either inodeId or mlId");
    }
    return new MLAssetQueryParams(projectId, inodeId, mlId, withAppState);
  }
}
