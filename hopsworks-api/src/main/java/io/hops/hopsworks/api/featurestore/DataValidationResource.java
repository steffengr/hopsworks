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

import io.hops.hopsworks.api.featurestore.json.datavalidation.ConstraintGroupDTO;
import io.hops.hopsworks.api.featurestore.json.datavalidation.DataValidationSettingsDTO;
import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.jwt.JWTHelper;
import io.hops.hopsworks.common.dao.featurestore.Featurestore;
import io.hops.hopsworks.common.dao.featurestore.FeaturestoreFacade;
import io.hops.hopsworks.common.dao.featurestore.datavalidation.ConstraintGroup;
import io.hops.hopsworks.common.dao.featurestore.datavalidation.DataValidationController;
import io.hops.hopsworks.common.dao.featurestore.featuregroup.FeaturegroupController;
import io.hops.hopsworks.common.dao.featurestore.featuregroup.FeaturegroupDTO;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.FeaturestoreException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
@JWTRequired(acceptedTokens={Audience.API}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
@AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
public class DataValidationResource {
  private static Logger LOGGER = Logger.getLogger(DataValidationResource.class.getName());
  
  @EJB
  private FeaturestoreFacade featurestoreFacade;
  @EJB
  private FeaturegroupController featuregroupController;
  @EJB
  private DataValidationConstraintsBuilder dataValidationConstraintsBuilder;
  @EJB
  private DataValidationController dataValidationController;
  @EJB
  private JWTHelper jwtHelper;
  @EJB
  private Settings settings;
  
  private Featurestore featurestore;
  private String path2hopsverification;
  private static final String MAIN_CLASS = "io.hops.hopsworks.verification.Verification";
  
  public DataValidationResource setFeatureStore(Integer featureStoreId) {
    this.featurestore = featurestoreFacade.findById(featureStoreId);
    return this;
  }
  
  @PostConstruct
  public void init() {
    // TODO(Antonis): Get everything from settings
    path2hopsverification = "hdfs:///user" + org.apache.hadoop.fs.Path.SEPARATOR + settings.getSparkUser()
        + org.apache.hadoop.fs.Path.SEPARATOR + "hops-verification-assembly-1.0.0-SNAPSHOT.jar";
  }
  
  @POST
  @Path("{featuregroupId}/rules")
  @Produces(MediaType.APPLICATION_JSON)
  public Response addValidationRules(ConstraintGroupDTO constraintGroups,
      @PathParam("featuregroupId") Integer featureGroupId,
      @Context SecurityContext sc) throws FeaturestoreException {
    Users user = jwtHelper.getUserPrincipal(sc);
    FeaturegroupDTO featureGroup = featuregroupController.getFeaturegroupWithIdAndFeaturestore(featurestore,
        featureGroupId);
  
    String rulesPath = dataValidationController.writeRulesToFile(user, featurestore.getProject(), featureGroup,
        constraintGroups.toConstraintGroups());
    
    DataValidationSettingsDTO settings = new DataValidationSettingsDTO();
    settings.setValidationRulesPath(rulesPath);
    settings.setExecutablePath(path2hopsverification);
    settings.setExecutableMainClass(MAIN_CLASS);
    
    return Response.ok().entity(settings).build();
  }
  
  @GET
  @Path("{featuregroupId}/rules")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getValidationRules(@PathParam("featuregroupId") Integer featureGroupId,
      @Context SecurityContext sc) throws FeaturestoreException {
    Users user = jwtHelper.getUserPrincipal(sc);
    FeaturegroupDTO featureGroup = featuregroupController.getFeaturegroupWithIdAndFeaturestore(featurestore,
        featureGroupId);
    List<ConstraintGroup> constraintGroups = dataValidationController.readRulesForFeatureGroup(user,
        featurestore.getProject(), featureGroup);
    ConstraintGroupDTO response = ConstraintGroupDTO.fromConstraintGroups(constraintGroups);
    return Response.ok(response).build();
  }
}
