/*
 * This file is part of Hopsworks
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

package io.hops.hopsworks.api.featurestore;

import io.hops.hopsworks.api.featurestore.json.datavalidation.ConstraintDTO;
import io.hops.hopsworks.api.featurestore.json.datavalidation.ConstraintGroupDTO;
import io.hops.hopsworks.common.dao.featurestore.datavalidation.ConstraintGroupLevel;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.Arrays;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class DataValidationConstraintsBuilder {
  
  public ConstraintGroupDTO build() {
    ConstraintDTO dto = new ConstraintDTO();
    
    for (int i = 0; i < 5; i++) {
      ConstraintDTO cdto = new ConstraintDTO();
      cdto.setName("constraint-" + i);
      cdto.setMin(2);
      cdto.setColumns(Arrays.asList("sf", "sdf", "sdf"));
      dto.addItem(cdto);
    }
    
    ConstraintGroupDTO cgdto1 = new ConstraintGroupDTO();
    cgdto1.setName("ConstraintGroup1");
    cgdto1.setDescription("SomeDescription");
    cgdto1.setConstraints(dto);
    
    ConstraintGroupDTO cgdto2 = new ConstraintGroupDTO();
    cgdto2.setName("COnstraintGroup2");
    cgdto2.setDescription("some_description");
    cgdto2.setLevel(ConstraintGroupLevel.Error);
    
    ConstraintGroupDTO cdto = new ConstraintGroupDTO();
    cdto.addItem(cgdto1);
    cdto.addItem(cgdto2);
    
    return cdto;
  }
}
