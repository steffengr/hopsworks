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

package io.hops.hopsworks.common.dao.featurestore.datavalidation;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class ConstraintGroupDeserializer implements JsonDeserializer<ConstraintGroup> {
  @Override
  public ConstraintGroup deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    ConstraintGroup constraintGroup = new ConstraintGroup();
    
    JsonObject constraintGroupJSON = jsonElement.getAsJsonObject();
    String groupName = constraintGroupJSON.getAsJsonPrimitive("name").getAsString();
    String groupLevelStr = constraintGroupJSON.getAsJsonPrimitive("level").getAsString();
    ConstraintGroupLevel groupLevel = ConstraintGroupLevel.valueOf(groupLevelStr);
    String groupDescription = constraintGroupJSON.getAsJsonPrimitive("description").getAsString();
    constraintGroup.setName(groupName);
    constraintGroup.setLevel(groupLevel);
    constraintGroup.setDescription(groupDescription);
    
    JsonArray constraintsJSON = constraintGroupJSON.getAsJsonArray("constraints");
    List<Constraint> constraints = new ArrayList<>(constraintsJSON.size());
    
    constraintsJSON.forEach(cj -> {
      JsonObject constraintJSON = cj.getAsJsonObject();
      String constraintName = constraintJSON.get("name").getAsString();
      String constraintHint = constraintJSON.get("hint").getAsString();
      Integer constraintMin = constraintJSON.get("min").getAsInt();
      Integer constraintMax = constraintJSON.get("max").getAsInt();
      JsonArray columnsJSON = constraintJSON.getAsJsonArray("columns");
      List<String> constraintColumns = new ArrayList<>(columnsJSON.size());
      columnsJSON.forEach(ccj -> constraintColumns.add(ccj.getAsString()));
      
      Constraint constraint = new Constraint();
      constraint.setName(constraintName);
      constraint.setHint(constraintHint);
      constraint.setMin(constraintMin);
      constraint.setMax(constraintMax);
      constraint.setColumns(constraintColumns);
      
      constraints.add(constraint);
    });
    constraintGroup.setConstraints(constraints);
    return constraintGroup;
  }
}
