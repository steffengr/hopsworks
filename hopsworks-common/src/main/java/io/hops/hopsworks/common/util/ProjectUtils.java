/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
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
 *
 * Changes to this file committed before and including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.hops.hopsworks.common.util;

import io.hops.hopsworks.common.dao.project.Project;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.ArrayList;
import java.util.List;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProjectUtils {
  
  public boolean isReservedProjectName(String projName) {
    boolean res = false;
    for (String name : getReservedProjectNames()) {
      if (name.compareToIgnoreCase(projName) == 0) {
        res = true;
        break;
      }
    }
    return res;
  }
  
  public List<String> getReservedProjectNames() {
    List<String> reservedNames = new ArrayList<>();
    reservedNames.add("python27");
    reservedNames.add("python36");
    reservedNames.add("python37");
    reservedNames.add("python38");
    reservedNames.add("python39");
    reservedNames.add("hops-system");
    return reservedNames;
  }
  
  public String getCurrentCondaEnvironment(Project project) {
    String condaEnv = project.getName();
    
    if (project.getConda() && !project.getCondaEnv()) {
      if (project.getPythonVersion().compareToIgnoreCase("2.7") == 0) {
        condaEnv = "python27";
      } else if (project.getPythonVersion().compareToIgnoreCase("3.6") == 0) {
        condaEnv = "python36";
      } else {
        throw new IllegalArgumentException("Error. Python has not been enabled for this project.");
      }
    }
    return condaEnv;
  }

  public String getCurrentCondaBaseEnvironment(Project project) {
    if (project.getPythonVersion().compareToIgnoreCase("2.7") == 0) {
      return "python27";
    } else if (project.getPythonVersion().compareToIgnoreCase("3.6") == 0) {
      return "python36";
    } else {
      throw new IllegalArgumentException("Error. Python has not been enabled for this project.");
    }
  }
}