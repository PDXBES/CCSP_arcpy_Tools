import os
import arcpy
import json
import traceback
from businessclasses.config import Config
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWRITE, S_IWGRP, S_IWOTH
try:
    from typing import List, Any
except:
    pass
from businessclasses.model import Model
from businessclasses.simulation import Simulation
from businessclasses.model_catalog_exception import InvalidModelException
from businessclasses.model_catalog_exception import InvalidModelPathException
from businessclasses.model_catalog_exception import InvalidModelPurposeException
from db_data_io import DbDataIo
from object_data_io import ObjectDataIo
from businessclasses.model_alteration import ModelAlteration
from businessclasses.project_type import ProjectType

class ModelDataIo(ObjectDataIo):
    def __init__(self, config, db_data_io):
        # type: (Config, DbDataIo) -> None
        self.config = config
        self.db_data_io = db_data_io

    def create_model_geometry(self, model):
        if model.valid:
            model_name = "Links"
            model_in = model.model_path + "\\" + "EmgaatsModel.gdb" + "\\Network\\" + model_name
            model_out = "in_memory\\" + model_name
            arcpy.Dissolve_management(model_in, model_out, "", "", "MULTI_PART")
            cursor = arcpy.da.SearchCursor(model_out, ["Shape@"])
            for row in cursor:
                geometry = row[0]
                model.model_geometry = geometry
            del cursor
            arcpy.Delete_management(model_out)
        else:
            raise InvalidModelException

    def read_simulations(self, model):
        # type: (Model) -> List[Simulation]
        simulations = []  # type: List[Simulation]
        simulation_descriptions = os.walk(model.simulation_folder_path()).next()[1]
        for simulation_description in simulation_descriptions:
            simulation = Simulation.initialize_with_current_id(self.config, self.db_data_io)
            simulation.model_path = model.model_path
            if simulation_description in self.config.standard_simulation_names():
                simulation_desc_parts = simulation_description.split("-")
                storm_type = simulation_desc_parts[0][0]
                storm_name = simulation_desc_parts[0][1:]
                dev_scenario = ""
                if len(simulation_desc_parts) == 1:
                    dev_scenario = "EX"
                else:
                    dev_scenario = simulation_desc_parts[1]
            else:
                dev_scenario = "EX"
                storm_type = "U"
                storm_name = "User-defined"
            simulation.storm_id = self.config.storm_id[(storm_name, storm_type)]
            simulation.dev_scenario_id = self.config.dev_scenario_id[dev_scenario]
            simulation.sim_desc = simulation_description
            simulations.append(simulation)
        return simulations

    def append_simulation(self, model_id, simulation):
        # type: (int, Simulation) -> None
        self.append_object_to_db(model_id, simulation, simulation.input_field_attribute_lookup, self.config.simulation_sde_path)

    def append_simulations(self, model):
        # type: (Model) -> None
        for simulation in model.simulations:
            self.append_simulation(model.id, simulation)

    def append_model_alteration(self, model_id, model_alteration):
        # type: (int, ModelAlteration) -> None
        if model_alteration.name == "model_alt_bc":
            self.append_object_to_db(model_id, model_alteration, model_alteration.input_field_attribute_lookup, self.config.model_alt_bc_sde_path)
        elif model_alteration.name == "model_alt_hydrologic":
            self.append_object_to_db(model_id, model_alteration, model_alteration.input_field_attribute_lookup, self.config.model_alt_hydrologic_sde_path)
        elif model_alteration.name == "model_alt_hydraulic":
            self.append_object_to_db(model_id, model_alteration, model_alteration.input_field_attribute_lookup, self.config.model_alt_hydraulic_sde_path)

    def append_model_alterations(self, model):
        # type: (Model) -> None
        for model_alteration in model.model_alterations:
            self.append_model_alteration(model.id, model_alteration)

    def append_project_type(self, model_id, project_type):
        # type: (int, ProjectType) -> None
        self.append_object_to_db(model_id, project_type, project_type.input_field_attribute_lookup,
                                 self.config.project_type_sde_path)

    def append_project_types(self, model):
        # type: (Model) -> None
        for project_type in model.project_types:
            self.append_project_type(model.id, project_type)

    def set_registered_model_to_read_only(self, model):
        # "https://stackoverflow.com/questions/28492685/change-file-to-read-only-mode-in-python"

        if model.valid_emgaats_model_structure() == True:
            model_path = model.model_path
            for root, directories, filenames in os.walk(model_path):
                for filename in filenames:
                    filepath = os.path.join(root, filename)
                    os.chmod(filepath, S_IREAD | S_IRGRP | S_IROTH)
        else:
            raise InvalidModelPathException

    def write_model_registration_file(self, model):
        pass
        # https://stackoverflow.com/questions/12309269/how-do-i-write-json-data-to-a-file

        # check that a registration file does not already exist - in different function?
        # TODO Model Status - done in model_catalog_db_data_io.add_model?
        model_registration_data = {"id": model.id,
                                   "create_date": model.create_date.strftime("%d-%b-%Y (%H:%M:%S.%f)"),
                                   "model_purpose_id": model.model_purpose_id,
                                   "model_purpose": self.config.model_purpose[model.model_purpose_id]}

        file_path = model.model_path
        file_name = "model_registration.json"
        model_registration_file = os.path.join(file_path, file_name)
        outfile = open(model_registration_file, 'w')
        try:
            json.dump(model_registration_data, outfile)
        except:
            traceback.print_exc()
            pass
        #TODO figure out how to make this testable
        outfile.close()

    #TODO: split this function to take a model registration path and be more "generalic"?
    def read_model_id_from_model_registration_file(self, model):
        registration_file = os.path.join(model.parent_model_path, "model_registration.json")
        with open(registration_file) as json_file:
            data = json.load(json_file)
            return data["id"]

    def read_model_purpose_from_model_registration_file(self, model):
        valid_model_purpose_values = self.config.model_purpose_id.keys()
        registration_file = os.path.join(model.parent_model_path, "model_registration.json")
        with open(registration_file) as json_file:
            data = json.load(json_file)
            if data["model_purpose"] in valid_model_purpose_values:
                return data["model_purpose"]
            else:
                raise InvalidModelPurposeException(None)


# TODO: finish the below functions
    def read_extraction_date_from_emgaats_config_file(self):
        pass
    #get list of emgaats data for all models in the folder
    #use brents existing xml notes
    #will return the list

    def read_deploy_date_and_results_extracted_date_from_emgaats_config_file(self):
        pass
    #get list of emgaats data for all simulations in the folder
    #will return the list
    #awaiting changes from Arnel
    # Q - is this duplicating read_extraction_date_from_emgaats_config_file or a separate thing? (DCA)

    def read_run_date_from_emgaats_config_file(self):
        pass
    #similar to above 2 functions

    def set_model_to_read_write(self, model):
        # "https://stackoverflow.com/questions/28492685/change-file-to-read-only-mode-in-python"

        model_path = model.model_path
        for root, directories, filenames in os.walk(model_path):
            for filename in filenames:
                filepath = os.path.join(root, filename)
                os.chmod(filepath, S_IWRITE | S_IWGRP | S_IWOTH )

    def check_model_is_read_only(self, model):
        model_path = model.model_path
        for root, directories, filenames in os.walk(model_path):
            for filename in filenames:
                filepath = os.path.join(root, filename)
                if os.access(filepath, os.W_OK):
                    return False

        return True






