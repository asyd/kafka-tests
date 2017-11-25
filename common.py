import os
import yaml


def config_load():
    """
    Load configuration from config.yaml
    :return: A dictionary
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    with open('{0}/conf/config.yaml'.format(current_dir), 'r') as fh:
        config = yaml.load(fh)

    return config
