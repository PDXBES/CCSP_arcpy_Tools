import os
import arcpy
from config import Config
from generic_object import GenericObject
from collections import OrderedDict


try:
    from typing import List, Any
except:
    pass


class DmePipe(GenericObject):

    simulations = None  # type: List[Simulation]

    def __init__(self, config):
        # type: (Config) -> None
        self.id = 0
        self.name = "dme_pipe"

    @staticmethod
    def input_field_attribute_lookup():
        field_attribute_lookup = OrderedDict()
        field_attribute_lookup["Model_ID"] = "id"
        field_attribute_lookup["Parent_Model_ID"] = "parent_model_id"
        field_attribute_lookup["Model_Request_ID"] = "model_request_id"
        field_attribute_lookup["Project_Phase_ID"] = "project_phase_id"
        field_attribute_lookup["Engine_Type_ID"] = "engine_type_id"
        field_attribute_lookup["Create_Date"] = "create_date"
        field_attribute_lookup["Created_by"] = "created_by"
        field_attribute_lookup["Deploy_Date"] = "deploy_date"
        field_attribute_lookup["Extract_Date"] = "extract_date"
        field_attribute_lookup["Run_Date"] = "run_date"
        field_attribute_lookup["Model_Path"] = "model_path"
        field_attribute_lookup["Model_Purpose_ID"] = "model_purpose_id"
        field_attribute_lookup["Model_Calibration_file"] = "model_calibration_file"
        field_attribute_lookup["Model_Status_ID"] = "model_status_id"
        field_attribute_lookup["Model_Alteration_file"] = "model_alteration_file"
        field_attribute_lookup["Project_Num"] = "project_num"
        field_attribute_lookup["Shape@"] = "model_geometry"
        return field_attribute_lookup

#TODO: move dataIO functions to a DataIO class; Validate results.gdb;
    @property
    def valid(self):
        if self.config.model_status[self.model_status_id] == "Working":
            if self.valid_emgaats_model_structure():
                return True
        elif self.config.model_status[self.model_status_id] == "Final":
            if self.project_phase_id == self.config.proj_phase_id["Pre Design"] or \
                    self.project_phase_id == self.config.proj_phase_id["Design 30"] or \
                    self.project_phase_id == self.config.proj_phase_id["Design 60"]:
                if self.valid_emgaats_model_structure():
                    return True
            elif self.project_phase_id == self.config.proj_phase_id["Design 90"]:
                if self.valid_emgaats_model_structure() and self.valid_required_simulations():
                    return True
            elif self.project_phase_id == self.config.proj_phase_id["Planning"]:
                if self.model_purpose_id == self.config.model_purpose_id["Calibration"]:
                    if self.valid_emgaats_model_structure() and self.valid_calibration_simulations():
                        return True
                    else:
                        self.valid_calibration_model_diagnostic()
                elif self.model_purpose_id == self.config.model_purpose_id["Characterization"]:
                    if self.valid_emgaats_model_structure() and self.valid_required_simulations():
                        return True
                elif self.model_purpose_id == self.config.model_purpose_id["Alternative"]:
                    if self.valid_emgaats_model_structure() and self.valid_required_simulations():
                        return True
                elif self.model_purpose_id == self.config.model_purpose_id["Recommended Plan"]:
                    if self.valid_emgaats_model_structure() and self.valid_required_simulations():
                        return True
            return False

    def valid_calibration_model_diagnostic(self):
        arcpy.AddError("Invalid Calibration Model")
        self._write_attributes_to_screen()
        arcpy.AddError("Simulations: ")
        for simulation in self.simulations:
            arcpy.AddError(simulation.valid())
            simulation._write_attributes_to_screen()
        #TODO: later - loop through and test on each sim, provide specific info on issue

    def validate_model_path(self):
        valid_model_path = os.path.exists(self.model_path)
        return valid_model_path

    def validate_config_file(self):
        self.config_file_path = self.model_path + "\\" + "emgaats.config"
        config_file_valid = os.path.isfile(self.config_file_path)
        return config_file_valid

    def validate_gdb(self):
        self.gdb_file_path = self.model_path + "\\" + "EmgaatsModel.gdb"
        gdb_file_valid = os.path.exists(self.gdb_file_path)
        return gdb_file_valid

    def simulation_folder_path(self):
        sim_file_path = self.model_path + "\\" + "sim"
        return sim_file_path

    def validate_sim(self):
        self.sim_file_path = self.simulation_folder_path()
        sim_folder_valid = os.path.exists(self.sim_file_path)
        return sim_folder_valid

    def valid_emgaats_model_structure(self):
        if self.validate_model_path():
            is_model_path_valid = True
        else:
            is_model_path_valid = False
        if self.validate_config_file():
            is_config_file_valid = True
        else:
            is_config_file_valid = False
        if self.validate_gdb():
            is_gdb_valid = True
        else:
            is_gdb_valid = False
        if self.validate_sim():
            is_sim_valid = True
        else:
            is_sim_valid = False
        if not is_model_path_valid or not is_config_file_valid or not is_gdb_valid or not is_sim_valid:
            is_valid = False
        else:
            is_valid = True
        return is_valid

    def valid_calibration_simulations(self):
        """Checks for user defined storms(id=0) and then checks for standard ccsp storm naming convention (8 characters, 3 for basin, the rest for date) """
        for simulation in self.simulations:
            if simulation.storm_id == 0:
                if len(simulation.sim_desc) == 8:
                    try:
                        sim_year = int(simulation.sim_desc[0:4])
                        sim_month = int(simulation.sim_desc[4:6])
                        sim_day = int(simulation.sim_desc[6:8])
                        current_date = datetime.datetime.now()
                        simulation_date = datetime.datetime(year=sim_year, month=sim_month, day=sim_day)
                        if simulation_date < current_date:
                            return True
                    except:
                        raise InvalidCalibrationStormSimulationDescription()
        return False

    def valid_required_simulations(self):
        required_storm_and_dev_scenario_ids = self.required_storm_and_dev_scenario_ids()

        for required_storm_id, required_dev_scenario in required_storm_and_dev_scenario_ids:
            required_simulation_found = False
            for simulation in self.simulations:
                if required_storm_id == simulation.storm_id and required_dev_scenario == simulation.dev_scenario_id:
                    required_simulation_found = True
            if required_simulation_found == False:
                return False
        return True

    def valid_parent_model_registration_file(self):
        self.parent_model_registration_file_path = os.path.join(self.parent_model_path, "model_registration.json")
        parent_model_registration_file_valid = os.path.isfile(self.parent_model_registration_file_path)
        return parent_model_registration_file_valid

    def required_storm_and_dev_scenario_ids(self):
        if self.project_phase_id == self.config.proj_phase_id["Planning"]:
            if self.model_purpose_id == self.config.model_purpose_id["Characterization"]:
                required_storm_and_dev_scenario_ids = self.config.ccsp_characterization_storm_and_dev_scenario_ids
            elif self.model_purpose_id == self.config.model_purpose_id["Alternative"]:
                required_storm_and_dev_scenario_ids = self.config.ccsp_alternative_storm_and_dev_scenario_ids
            elif self.model_purpose_id == self.config.model_purpose_id["Recommended Plan"]:
                required_storm_and_dev_scenario_ids = self.config.ccsp_recommended_plan_storm_and_dev_scenario_ids

            else:
                raise InvalidModelPurposeException
        elif self.project_phase_id == self.config.proj_phase_id["Design 90"]:

            if self.model_purpose_id == self.config.model_purpose_id["Characterization"]:
                required_storm_and_dev_scenario_ids = self.config.ccsp_characterization_storm_and_dev_scenario_ids
                # TODO: create test for Design 90 and required_storm_and_dev_scenario_ids in config

        else:
            raise InvalidProjectPhase
        return required_storm_and_dev_scenario_ids

    def model_valid_diagnostic(self):
        self.valid_emgaats_model_structure_diagnostic()

    def valid_emgaats_model_structure_diagnostic(self):
        if self.validate_model_path():
            arcpy.AddMessage("Model Path is valid: " + self.model_path)
        else:
            arcpy.AddMessage("Model Path is not valid: " + self.model_path)
        if self.validate_config_file():
            arcpy.AddMessage("Config File is valid: " + self.config_file_path)
        else:
            arcpy.AddMessage("Config File is not valid: " + self.config_file_path)
        if self.validate_gdb():
            arcpy.AddMessage("GDB is valid: " + self.gdb_file_path)

        else:
            arcpy.AddMessage("GDB is not valid: " + self.gdb_file_path)
        if self.validate_sim():
            arcpy.AddMessage("Sim folder is valid: " + self.sim_file_path)

        else:
            arcpy.AddMessage("Sim folder is not valid: " + self.sim_file_path)

    def create_model_alt_bc(self, alteration_type, modelcatalog_db_data_io):
        model_alt_bc = ModelAltBc.initialize_with_current_id(self.config, modelcatalog_db_data_io)
        model_alt_bc.model_alteration_type_id = self.config.model_alt_bc_id[alteration_type]
        return model_alt_bc

    def create_model_alt_hydrologic(self, alteration_type, modelcatalog_db_data_io):
        model_alt_hydrologic = ModelAltHydrologic.initialize_with_current_id(self.config, modelcatalog_db_data_io)
        model_alt_hydrologic.model_alteration_type_id = self.config.model_alt_hydrologic_id[alteration_type]
        return model_alt_hydrologic

    def create_model_alt_hydraulic(self, alteration_type, modelcatalog_db_data_io):
        model_alt_hydraulic = ModelAltHydraulic.initialize_with_current_id(self.config, modelcatalog_db_data_io)
        model_alt_hydraulic.model_alteration_type_id = self.config.model_alt_hydraulic_id[alteration_type]
        return model_alt_hydraulic

    def create_model_alterations(self, alteration_types, alteration_category, modelcatalog_db_data_io):
        if alteration_types is None:
            pass
        else:
            for alteration_type in alteration_types:
                if alteration_category == "bc":
                    model_alteration = self.create_model_alt_bc(alteration_type[0], modelcatalog_db_data_io)
                    self.model_alterations.append(model_alteration)
                elif alteration_category == "hydrologic":
                    model_alteration = self.create_model_alt_hydrologic(alteration_type[0], modelcatalog_db_data_io)
                    self.model_alterations.append(model_alteration)
                elif alteration_category == "hydraulic":
                    model_alteration = self.create_model_alt_hydraulic(alteration_type[0], modelcatalog_db_data_io)
                    self.model_alterations.append(model_alteration)

    def create_model_alterations_bc(self, alteration_types, modelcatalog_db_data_io):
        self.create_model_alterations(alteration_types, "bc", modelcatalog_db_data_io)

    def create_model_alterations_hydrologic(self, alteration_types, modelcatalog_db_data_io):
        self.create_model_alterations(alteration_types, "hydrologic", modelcatalog_db_data_io)

    def create_model_alterations_hydraulic(self, alteration_types, modelcatalog_db_data_io):
        self.create_model_alterations(alteration_types, "hydraulic", modelcatalog_db_data_io)

    def create_project_type(self, project_type_name, modelcatalog_db_data_io):
        # type: (str)->ProjectType
        project_type = ProjectType.initialize_with_current_id(self.config, modelcatalog_db_data_io)
        project_type.project_type_id = self.config.proj_type_id[project_type_name]
        project_type.parent_id = self.id
        return project_type

    def create_project_types(self, project_types, modelcatalog_db_data_io):
        # type: (str)->None
        for project_type_name in project_types:
            project_type = self.create_project_type(project_type_name, modelcatalog_db_data_io)
            self.project_types.append(project_type)

    def create_simulations(self, model_data_io):
        self.simulations = model_data_io.read_simulations(self)

    # TODO: Create tests for add_project and add_project_types

    def ready_to_register(self):
        if self.config.model_status[self.model_status_id] == "Working":
            return self.valid
        return False
    #TODO : change this to call a diagnostic method

    def write_to_rrad(self):
        if self.valid:
            if self.model_status_id == self.config.model_status_id["Final"]:
                if self.project_phase_id == self.config.proj_phase_id["Planning"] or self.project_phase_id == self.config.proj_phase_id["Design 90"]:
                    if self.model_purpose_id != self.config.model_purpose_id["Calibration"]:
                        return True
        return False


    def set_parent_model_id(self, model_data_io):
        valid_model_purpose_values = self.config.model_purpose_id.values()
        if self.model_purpose_id in valid_model_purpose_values:
            if self.model_purpose_id != self.config.model_purpose_id["Calibration"]:
                if self.valid_parent_model_registration_file():
                    parent_model_id = model_data_io.read_model_id_from_model_registration_file(self)

                    parent_model_purpose = model_data_io.read_model_purpose_from_model_registration_file(self)
                    if self.valid_parent_model_purpose(parent_model_purpose):
                        self.parent_model_id = parent_model_id
                    else:
                        raise InvalidParentModelPurposeException(parent_model_purpose)
                else:
                    raise InvalidModelRegistrationFileException
            else:
                self.parent_model_id = None
        else:
            raise InvalidModelPurposeException(None)


    def valid_parent_model_purpose(self, parent_model_purpose):

        if self.config.model_purpose[self.model_purpose_id] == "Characterization":
            if parent_model_purpose == "Calibration":
                return True
        if self.config.model_purpose[self.model_purpose_id] == "Calibration":
            if parent_model_purpose == None:
                return True
        if self.config.model_purpose[self.model_purpose_id] == "Alternative":
            if parent_model_purpose == "Characterization":
                return True
        if self.config.model_purpose[self.model_purpose_id] == "Recommended Plan":
            if parent_model_purpose == "Characterization":
                return True
        return False





