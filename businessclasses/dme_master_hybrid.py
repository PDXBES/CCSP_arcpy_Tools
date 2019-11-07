try:
    from typing import List, Any
except:
    pass
from model import Model
from simulation import Simulation
from model_alt_bc import ModelAltBc
from model_alt_hydraulic import ModelAltHydraulic
from model_alt_hydrologic import ModelAltHydrologic
from project_type import ProjectType
from model_catalog_exception import InvalidModelException, DuplicateModelException, DuplicatesInInputModeList
from config import Config
from dataio.model_catalog_db_data_io import ModelCatalogDbDataIo


class ModelCatalog:
    models = None  # type: List[Model]

    def __init__(self, config):
        # type: (Config) -> None
        self.models = []
        self.config = config

    def add_model(self, model):
        # type: (Model) -> None
        self.check_for_duplicate_model(model)
        self.check_for_valid_model(model)
        self.models.append(model)

    def remove_model(self):
        self.models.remove(self.models[0])

    def add_models(self, models):
        # type: (List[Model]) -> None
        for model in models:
            self.check_for_duplicate_model(model)
        for model in models:
            self.check_for_valid_model(model)

        self.check_for_duplicates_in_input_model_list(models)

        for model in models:
            self.add_model(model) # add_model already runs check for duplicate and check valid - why do it twice? (DCA)

    def check_for_duplicates_in_input_model_list(self, models):
        # type: (List[Model]) -> None
        model_set = set()
        for model in models:
            if model not in model_set:
                model_set.add(model)
            else:
                raise DuplicatesInInputModeList

    def check_for_duplicate_model(self, model):
        # type: (Model) -> None
        if model in self.models:
            raise DuplicateModelException

    def check_for_valid_model(self, model):
        # type: (Model) -> None
        if not model.valid:
            raise InvalidModelException

    def create_models_with_tracking_data_only_from_model_catalog_db(self, model_catalog_db_data_io):
        # type: (ModelCatalogDbDataIo) -> List[Model]
        input_table_name = self.config.model_tracking_sde_path
        class_type = "model"
        models = model_catalog_db_data_io.create_objects_from_database(class_type, input_table_name)
        return models

    def create_simulations_from_model_catalog_db(self, model_catalog_db_data_io):
        # type: (ModelCatalogDbDataIo) -> List[Simulation]
        input_table_name = self.config.simulation_sde_path
        class_type = "simulation"
        simulations = model_catalog_db_data_io.create_objects_from_database(class_type, input_table_name)
        return simulations

    def create_model_alt_bcs_from_model_catalog_db(self, model_catalog_db_data_io):
        # type: (ModelCatalogDbDataIo) -> List[ModelAltBc]
        input_table_name = self.config.model_alt_bc_sde_path
        class_type = "model_alt_bc"
        model_alt_bcs = model_catalog_db_data_io.create_objects_from_database(class_type, input_table_name)
        return model_alt_bcs

    def create_model_alt_hydraulics_from_model_catalog_db(self, model_catalog_db_data_io):
        # type: (ModelCatalogDbDataIo) -> List[ModelAltHydraulic]
        input_table_name = self.config.model_alt_hydraulic_sde_path
        class_type = "model_alt_hydraulic"
        model_alt_hydraulics = model_catalog_db_data_io.create_objects_from_database(class_type, input_table_name)
        return model_alt_hydraulics

    def create_model_alt_hydrologics_from_model_catalog_db(self, model_catalog_db_data_io):
        # type: (ModelCatalogDbDataIo) -> List[ModelAltHydrologic]
        input_table_name = self.config.model_alt_hydrologic_sde_path
        class_type = "model_alt_hydrologic"
        model_alt_hydrologics = model_catalog_db_data_io.create_objects_from_database(class_type, input_table_name)
        return model_alt_hydrologics

    def create_project_types_from_model_catalog_db(self, model_catalog_db_data_io):
        # type: (ModelCatalogDbDataIo) -> List[ProjectType]
        input_table_name = self.config.project_type_sde_path
        class_type = "project_type"
        project_types = model_catalog_db_data_io.create_objects_from_database(class_type, input_table_name)
        return project_types

    def create_models_from_model_catalog_db(self, model_catalog_db_data_io):
        # type: (ModelCatalogDbDataIo) -> List[Model]
        models = self.create_models_with_tracking_data_only_from_model_catalog_db(model_catalog_db_data_io)
        simulations = self.create_simulations_from_model_catalog_db(model_catalog_db_data_io)
        model_alterations = []
        model_alterations += self.create_model_alt_bcs_from_model_catalog_db(model_catalog_db_data_io)
        model_alterations += self.create_model_alt_hydraulics_from_model_catalog_db(model_catalog_db_data_io)
        model_alterations += self.create_model_alt_hydrologics_from_model_catalog_db(model_catalog_db_data_io)
        project_types = self.create_project_types_from_model_catalog_db(model_catalog_db_data_io)
        for model in models:
            for simulation in simulations:
                if simulation.parent_id == model.id:
                    model.simulations.append(simulation)
            for model_alteration in model_alterations:
                if model_alteration.parent_id == model.id:
                    model.model_alterations.append(model_alteration)
            for project_type in project_types:
                if project_type.parent_id == model.id:
                    model.project_types.append(project_type)
        return models

    def add_models_from_model_catalog_db(self, model_catalog_db_data_io):
        self.add_models(self.create_models_from_model_catalog_db(model_catalog_db_data_io))

    def calibration_models(self):
        calibration_models = []
        for model in self.models:
            if self.config.model_purpose[model.model_purpose_id] == "Calibration":
                calibration_models.append(model)
        return calibration_models

    def characterization_models(self):
        characterization_models = []
        for model in self.models:
            if self.config.model_purpose[model.model_purpose_id] == "Characterization":
                characterization_models.append(model)
        return characterization_models


