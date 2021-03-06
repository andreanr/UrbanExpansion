import luigi
import pdb
from luigi import configuration
from luigi.contrib import postgres

#from models.models_tasks import TrainModels
from models.cv_models import RunCVModels
import utils

class RunUrbanExpansion(luigi.WrapperTask):
    experiment_path = configuration.get_config().get('general','experiment_path')

    def requires(self):
        experiment = utils.read_yaml(self.experiment_path)
        features = utils.get_features(experiment)
        models = experiment['models']
        parameters = experiment['parameters']
        return RunCVModels(features=features,
                           models=models,
                           parameters=parameters)

