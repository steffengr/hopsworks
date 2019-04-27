=begin
 This file is part of Hopsworks
 Copyright (C) 2019, Logical Clocks AB. All rights reserved

 Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 the GNU Affero General Public License as published by the Free Software Foundation,
 either version 3 of the License, or (at your option) any later version.

 Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 PURPOSE.  See the GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/>.
=end
module ProvenanceHelper

  def prov_create_dir(project, dirname) 
    target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset"
    payload_string = '{"name": "' + dirname + '"}'
    payload = JSON.parse(payload_string)
    pp "post #{target}, #{payload}"
    post target, payload
    expect_status(200)
  end

  def prov_create_experiment(project, experiment_name) 
      pp "create experiment #{experiment_name} in project #{project[:inode_name]}"
      prov_create_dir(project, "Experiments/#{experiment_name}")
  end

  def prov_create_model(project, model_name) 
    pp "create model #{model_name} in project #{project[:inode_name]}"
    models = "Models"
    prov_create_dir(project, "#{models}/#{model_name}")
  end

  def prov_create_model_version(project, model_name, model_version) 
    pp "create model #{model_name}_#{model_version} in project #{project[:inode_name]}"
    models = "Models"
    prov_create_dir(project, "#{models}/#{model_name}/#{model_version}")
  end

  def prov_create_td(project, td_name, td_version) 
    pp "create training dataset #{td_name}_#{td_version} in project #{project[:inode_name]}"
    training_datasets = "#{project[:inode_name]}_Training_Datasets"
    prov_create_dir(project, "#{training_datasets}/#{td_name}_#{td_version}")
  end
  
  def prov_delete_dir(project, dirname) 
    target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset/file/#{dirname}"
    delete target
    expect_status(200)
  end

  def prov_delete_experiment(project, experiment_name) 
    pp "delete experiment #{experiment_name} in project #{project[:inode_name]}"
    experiments = "Experiments"
    prov_delete_dir(project, "#{experiments}/#{experiment_name}")
  end

  def prov_delete_model(project, model_name) 
    pp "delete model #{model_name} in project #{project[:inode_name]}"
    models = "Models"
    prov_delete_dir(project, "#{models}/#{model_name}")
  end

  def prov_delete_td(project, td_name, td_version) 
    pp "delete training dataset #{td_name}_#{td_version} in project #{project[:inode_name]}"
    training_datasets = "#{project[:inode_name]}_Training_Datasets"
    prov_delete_dir(project, "#{training_datasets}/#{td_name}_#{td_version}")
  end

  def prov_experiment_id(experiment_name)
    "#{experiment_name}"
  end

  def prov_model_id(model_name, model_version)
    "#{model_name}_#{model_version}"
  end

  def prov_td_id(td_name, td_version)
    "#{td_name}_#{td_version}"
  end

  def prov_get_td_record(project, td_name, td_version) 
    training_datasets = "#{project[:inode_name]}_Training_Datasets"
    training_dataset = prov_td_id(td_name, td_version)
    FileProv.where("project_name": project["inode_name"], "i_parent_name": training_datasets, "i_name": training_dataset)
  end

  def prov_add_xattr(original, xattr_name, xattr_value, xattr_op, increment)
    xattrRecord = original.dup
    xattrRecord["inode_operation"] = xattr_op
    xattrRecord["io_logical_time"] = original["io_logical_time"]+increment
    xattrRecord["io_timestamp"] = original["io_timestamp"]+increment
    xattrRecord["i_xattr_name"] = xattr_name
    xattrRecord["io_logical_time_batch"] = original["io_logical_time_batch"]+increment
    xattrRecord["io_timestamp_batch"] = original["io_timestamp_batch"]+increment
    xattrRecord.save!

    FileProvXAttr.create(inode_id: xattrRecord["inode_id"], namespace: 5, name: xattr_name, inode_logical_time: xattrRecord["io_logical_time"], value: xattr_value)
  end

  def prov_add_app_states1(app_id, user)
    timestamp = Time.now
    AppProv.create(id: app_id, state: "null", timestamp: timestamp, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "null", timestamp: timestamp+5, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
  end
  def prov_add_app_states2(app_id, user)
    timestamp = Time.now
    AppProv.create(id: app_id, state: "null", timestamp: timestamp, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "null", timestamp: timestamp+5, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "FINISHED", timestamp: timestamp+10, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: timestamp+50)
  end

  def prov_wait_for_epipe() 
    pp "waiting"
    sleepCounter1 = 0
    sleepCounter2 = 0
    until FileProv.all.empty? || sleepCounter1 == 10 do
      sleep(5)
      sleepCounter1 += 1
    end
    until AppProv.all.empty? || sleepCounter2 == 10 do
      sleep(5)
      sleepCounter2 += 1
    end
    sleep(5)
    expect(sleepCounter1).to be < 10
    expect(sleepCounter2).to be < 10
    pp "done waiting"
  end

  def prov_check_experiment3(experiments, experiment_id, currentState) 
    experiment = experiments.select { |e| e["mlId"] == experiment_id }
    expect(experiment.length).to eq 1

    #pp experiment[0]["appState"]
    expect(experiment[0]["appState"]["currentState"]).to eq currentState
  end

  def prov_check_asset_with_id(assets, asset_id) 
    asset = assets.select {|a| a["mlId"] == asset_id }
    expect(asset.length).to eq 1
    #pp asset
  end 

  def prov_check_asset_with_xattrs(assets, asset_id, xattrs) 
    asset = assets.select {|a| a["mlId"] == asset_id }
    expect(asset.length).to eq 1
    #pp model
    expect(asset[0]["xattrs"]["entry"].length).to eq xattrs.length
    xattrs.each do |key, value|
      #pp model[0]["xattrs"]["entry"]
      xattr = asset[0]["xattrs"]["entry"].select do |e| 
        e["key"] == key && e["value"] == value
      end
      expect(xattr.length).to eq 1
      #pp xattr
    end
  end 

  def get_ml_asset_in_project(project, ml_type, withAppState) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/mlType/#{ml_type}/list"
    query_params = "?withAppState=#{withAppState}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_ml_asset_by_id(project, ml_type, ml_id, withAppState, status) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/mlType/#{ml_type}/exact"
    query_params = "?mlId=#{ml_id}&withAppState=#{withAppState}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(status)
    result
  end

  def get_ml_asset_by_xattr_count(project, ml_type, xattr_key, xattr_val, count) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/mlType/#{ml_type}/list"
    query_params = "?xattrs=#{xattr_key}:#{xattr_val}&count=true"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    expect(parsed_result["result"]["value"]).to eq count
    parsed_result
  end
end