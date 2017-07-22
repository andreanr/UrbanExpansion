import luigi
import pdb
from luigi import configuration
from luigi.contrib import postgres

import utils
from model_tasks import TrainModels

class RunUrbanExpansion(luigi.Wrapper):
    experiment_path = configuration.get('general','experiment_path')

    def requieres(self):
        experiment = utils.utils.read_yaml(self.experiment_path)
        features = utils.get_features(experiment)
        models = experiment['models']
        parameters = experiment['parameters']
        return TrainModels(features=features,
                           models=models,
                           parameters=parameters)

