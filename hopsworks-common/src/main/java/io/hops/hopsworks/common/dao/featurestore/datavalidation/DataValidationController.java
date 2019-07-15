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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.hops.hopsworks.common.dao.featurestore.featuregroup.FeaturegroupDTO;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.FeaturestoreException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class DataValidationController {
  private static Logger LOGGER = Logger.getLogger(DataValidationController.class.getName());
  public static final String DATA_VALIDATION_DATASET = "DataValidation";
  private static final String PATH_TO_DATA_VALIDATION = Path.SEPARATOR + Settings.DIR_ROOT + Path.SEPARATOR
      + "%s" + Path.SEPARATOR + DATA_VALIDATION_DATASET;
  
  private static final String PATH_TO_DATA_VALIDATION_RESULTS = Path.SEPARATOR + Settings.DIR_ROOT + Path.SEPARATOR
      + "%s" + Path.SEPARATOR + DATA_VALIDATION_DATASET + Path.SEPARATOR + "%s" + Path.SEPARATOR + "%d";
  
  private static final String PATH_TO_DATA_VALIDATION_RULES = PATH_TO_DATA_VALIDATION + Path.SEPARATOR + "rules";
  private static final String PATH_TO_DATA_VALIDATION_RULES_FILE = PATH_TO_DATA_VALIDATION_RULES + Path.SEPARATOR
      + "%s_%d-rules.json";
  private static final String HDFS_FILE_PATH = "hdfs://%s";
  
  @EJB
  private DistributedFsService distributedFsService;
  @EJB
  private HdfsUsersController hdfsUsersController;
  @EJB
  private Settings settings;
  
  public void storeValidationRules() {
  }
  
  public String writeRulesToFile(Users user, Project project, FeaturegroupDTO featureGroup,
      List<ConstraintGroup> constraintGroups) throws FeaturestoreException {
    String jsonRules = convert2deequRules(constraintGroups);
    Path rulesPath = getDataValidationRulesFilePath(project, featureGroup.getName(), featureGroup.getVersion());
    writeToHDFS(project, user, rulesPath, jsonRules);
    return String.format(HDFS_FILE_PATH, rulesPath.toString());
  }
  
  public List<ConstraintGroup> readRulesForFeatureGroup(Users user, Project project, FeaturegroupDTO featureGroup)
    throws FeaturestoreException {
    String hdfsUsername = hdfsUsersController.getHdfsUserName(project, user);
    DistributedFileSystemOps udfso = null;
    try {
      Path path2rules = new Path(String.format(PATH_TO_DATA_VALIDATION_RULES, project.getName())
          + Path.SEPARATOR + "*");
      udfso = distributedFsService.getDfsOps(hdfsUsername);
      GlobFilter filter = new GlobFilter(featureGroup.getName() + "_*-rules.json");
      FileStatus[] rules = udfso.getFilesystem().globStatus(path2rules, filter);
      if (rules == null || rules.length == 0) {
        return Collections.EMPTY_LIST;
      }
      List<ConstraintGroup> constraintGroups = new ArrayList<>();
      for (int i = 0; i < rules.length; i++) {
        FileStatus fileStatus = rules[i];
        Path path2rule = fileStatus.getPath();
        try (FSDataInputStream inStream = udfso.open(path2rule)) {
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          IOUtils.copyBytes(inStream, out, 512);
          String content = out.toString("UTF-8");
          List<ConstraintGroup> groups = convertFromDeequRules(content);
          constraintGroups.addAll(groups);
          out.close();
        }
      }
      return constraintGroups;
    } catch (IOException ex) {
      throw new FeaturestoreException(RESTCodes.FeaturestoreErrorCode.COULD_NOT_CREATE_DATA_VALIDATION_RULES,
          Level.WARNING, "Failed to read validation rules",
          "Could not read validation rules from HDFS", ex);
    } finally {
      if (udfso != null) {
        distributedFsService.closeDfsClient(udfso);
      }
    }
  }
  
  private void writeToHDFS(Project project, Users user, Path path2file, String content) throws FeaturestoreException {
    String hdfsUsername = hdfsUsersController.getHdfsUserName(project, user);
    DistributedFileSystemOps udfso = null;
    try {
      udfso = distributedFsService.getDfsOps(hdfsUsername);
      try (FSDataOutputStream outStream = udfso.create(path2file)) {
        outStream.writeBytes(content);
        outStream.hflush();
      } catch (IOException ex) {
        throw new FeaturestoreException(RESTCodes.FeaturestoreErrorCode.COULD_NOT_CREATE_DATA_VALIDATION_RULES,
            Level.WARNING, "Failed to create data validation rules",
            "Could not write data validation rules to HDFS", ex);
      }
    } finally {
      if (udfso != null) {
        distributedFsService.closeDfsClient(udfso);
      }
    }
  }
  
  private Path getDataValidationRulesFilePath(Project project, String featureGroupName, Integer featureGroupVersion) {
    return new Path(String.format(PATH_TO_DATA_VALIDATION_RULES_FILE, project.getName(), featureGroupName,
        featureGroupVersion));
  }
  
  private String convert2deequRules(List<ConstraintGroup> constraintGroups) {
    Gson constraintGroupSerializer = new GsonBuilder()
        .registerTypeAdapter(ConstraintGroup.class, new ConstraintGroupSerializer())
        .create();
    JsonObject json = new JsonObject();
    JsonArray constraintGroupsJSON = new JsonArray();
    for (ConstraintGroup constraintGroup : constraintGroups) {
      JsonElement constraintGroupJSON = constraintGroupSerializer.toJsonTree(constraintGroup);
      constraintGroupsJSON.add(constraintGroupJSON);
    }
    json.add("constraintGroups", constraintGroupsJSON);
    return constraintGroupSerializer.toJson(json);
  }
  
  private List<ConstraintGroup> convertFromDeequRules(String rules) {
    Gson constraintGroupsDeserializer = new GsonBuilder()
        .registerTypeAdapter(ConstraintGroup.class, new ConstraintGroupDeserializer())
        .create();
    JsonElement topLevelObject = constraintGroupsDeserializer.fromJson(rules, JsonElement.class);
    JsonArray constraintGroupsJSON = topLevelObject.getAsJsonObject()
        .getAsJsonArray("constraintGroups");
    List<ConstraintGroup> constraintGroups = new ArrayList<>(constraintGroupsJSON.size());
    constraintGroupsJSON.forEach(cgj -> {
      ConstraintGroup cg = constraintGroupsDeserializer.fromJson(cgj, ConstraintGroup.class);
      constraintGroups.add(cg);
    });
    return constraintGroups;
  }
}