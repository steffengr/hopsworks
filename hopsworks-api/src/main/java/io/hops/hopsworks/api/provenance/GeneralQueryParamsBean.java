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

import io.hops.hopsworks.common.provenance.GeneralQueryParams;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;

public class GeneralQueryParamsBean {

  @QueryParam("count")
  @DefaultValue("false")
  private boolean count;

  public GeneralQueryParamsBean() {}

  public GeneralQueryParamsBean(@QueryParam("count") @DefaultValue("false") boolean count) {
    this.count = count;
  }

  public boolean isCount() {
    return count;
  }

  public void setCount(boolean count) {
    this.count = count;
  }

  @Override
  public String toString() {
    return "MLAssetListQueryParamsBean{"
      + " count=" + count
      + '}';
  }

  public GeneralQueryParams params() {
    return new GeneralQueryParams(count);
  }
  
  public static GeneralQueryParams none() {
    return new GeneralQueryParams(false);
  }
}
