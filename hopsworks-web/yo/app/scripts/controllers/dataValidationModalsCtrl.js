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
'use strict';
angular.module('hopsWorksApp')
    .controller('DataValidationModalsCtrl', ['$uibModalInstance', 'features', 'constraintGroups', 'growl',
        function ($uibModalInstance, features, constraintGroups, growl) {
            self = this;
            self.features = features;
            self.constraintGroups = constraintGroups;

            self.constraintGroupLevels = ['Warning', 'Error'];

            self.columnsModes = {
                NO_COLUMNS: 0,
                SINGLE_COLUMN: 1,
                MULTI_COLUMNS: 2
            }

            self.predicateType = {
                BOUNDARY: 0
            }

            var Predicate = function(name, predicateType, columnsSelectionMode) {
                this.name = name;
                this.predicateType = predicateType;
                this.constraintGroup = {};
                this.columnsSelectionMode = columnsSelectionMode;
                // For SIGNLE_COLUMN predicates
                this.feature;
                // For MULTI_COLUMN predicates
                this.features = [];
                this.hint = "";
            }

            Predicate.prototype.constructPredicate = function () {
                var features_names = [];
                if (this.columnsSelectionMode == self.columnsModes.NO_COLUMNS) {
                    features_names.push('*');
                } else if (this.columnsSelectionMode == self.columnsModes.MULTI_COLUMNS) {
                    for (var i = 0; i < this.features.length; i++) {
                        features_names.push(this.features[i].name)
                    }
                } else if (this.columnsSelectionMode == self.columnsModes.SINGLE_COLUMN) {
                    features_names.push(this.feature.name);
                }
                var args = {
                    hint: this.hint
                }
                if (this.predicateType == self.predicateType.BOUNDARY) {
                    args.min = this.min;
                    args.max = this.max;
                }

                var predicate = {
                    feature: features_names,
                    predicate: this.name,
                    arguments: args,
                    constraintGroup: this.constraintGroup
                }
                return predicate;
            };

            /*
            ** Deequ rules
            */
            var hasSize = new Predicate("hasSize", self.predicateType.BOUNDARY,
                self.columnsModes.NO_COLUMNS);
            var hasCompleteness = new Predicate("hasCompleteness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);
            var hasUniqueness = new Predicate("hasUniqueness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);
            var hasDistinctness = new Predicate("hasDistinctness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);
            var hasUniqueValueRatio = new Predicate("hasUniqueValueRatio",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);
            var hasNumberOfDistinctValues = new Predicate("hasNumberOfDistinctValues",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN);
            var hasEntropy = new Predicate("hasEntropy",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN);
            var hasMin = new Predicate("hasMin", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN);
            var hasMax = new Predicate("hasMax", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN);
            var hasMean = new Predicate("hasMean", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN);
            var hasSum = new Predicate("hasSum", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN);
            var hasStandardDeviation = new Predicate("hasStandardDeviation",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN);

            self.valid_predicates = [hasSize, hasCompleteness, hasUniqueness,
                hasDistinctness, hasUniqueValueRatio, hasNumberOfDistinctValues,
                hasEntropy, hasMin, hasMax, hasMean, hasSum, hasStandardDeviation];

            self.selected_predicate;

            self.selectedConstraintGroup;
        
            self.feature_selected = function () {
                console.log("Feature selected: " + self.selected_feature)
            }

            self.has_predicate_been_selected = function () {
                if (self.selected_predicate) {
                    return true
                }
                return false
            }

            self.addNewPredicate = function () {
                if (self.selected_predicate) {
                    var predicate = self.selected_predicate.constructPredicate();
                    growl.info('Added new predicate ' + predicate.predicate,
                      {title: 'Added predicate', ttl: 2000, referenceId: 1})
                    $uibModalInstance.close(predicate);
                } else {
                    $uibModalInstance.dismiss('cancel');
                }
                
            }

            self.addNewGroup = function () {
                console.log("Creating new data validation group");            
                $uibModalInstance.close(self.constraintGroup);
            }

            /**
             * Closes the modal
             */
            self.close = function () {
                self.selected_predicate = {};
                $uibModalInstance.dismiss('cancel');
            };
        }
    ]);