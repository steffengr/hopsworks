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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class ConstraintGroupSerializer implements JsonSerializer<ConstraintGroup> {
  @Override
  public JsonElement serialize(ConstraintGroup constraintGroup, Type type,
      JsonSerializationContext jsonSerializationContext) {
    
    JsonObject constraintGroupJSON = new JsonObject();
    constraintGroupJSON.add("name", new JsonPrimitive(constraintGroup.getName()));
    constraintGroupJSON.add("level", new JsonPrimitive(constraintGroup.getLevel().toString()));
    constraintGroupJSON.add("description", new JsonPrimitive(constraintGroup.getDescription()));
    
    JsonArray constraintsJSON = new JsonArray();
    for (Constraint c : constraintGroup.getConstraints()) {
      JsonObject constraintJSON = new JsonObject();
      constraintJSON.add("name", new JsonPrimitive(c.getName()));
      constraintJSON.add("hint", new JsonPrimitive(c.getHint()));
      constraintJSON.add("min", new JsonPrimitive(c.getMin()));
      constraintJSON.add("max", new JsonPrimitive(c.getMax()));
      JsonArray columns = new JsonArray();
      for (String column : c.getColumns()) {
        columns.add(new JsonPrimitive(column));
      }
      constraintJSON.add("columns", columns);
      
      constraintsJSON.add(constraintJSON);
    }
    constraintGroupJSON.add("constraints", constraintsJSON);
    
    return constraintGroupJSON;
  }
}
