from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.druid import Druid
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_druid(*args, **kwargs):
    """
    Template for loading data from a Druid warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#druid
    """
    query = 'your Druid query'  # Specify your SQL query here
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Druid.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
