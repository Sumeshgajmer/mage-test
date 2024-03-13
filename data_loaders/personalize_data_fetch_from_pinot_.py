from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.pinot import Pinot
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_pinot(*args, **kwargs):
    """
    Template for loading data from a Pinot warehouse.
    Specify your configuration settings in 'io_config.yaml'.
    Docs: https://docs.mage.ai/design/data-loading#pinot
    """

    execution_date_str = str(kwargs.get('execution_date').date())
    pattern_str = "%Y-%m-%d"

    parsed_execution_time_mills = get_time_millis_from_date(execution_date_str, pattern_str)
    execution_time_mills_a_day_ago = parsed_execution_time_mills - 86400000

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Pinot.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return get_paginated_active_users(loader)

def get_paginated_active_users(loader, parsed_execution_time_mills, execution_time_mills_a_day_ago):
    offset = 0
    limit = 5
    active_user_list = pd.DataFrame()  

    while True:
        query = build_query(parsed_execution_time_mills, execution_time_mills_a_day_ago, limit, offset)
        result_from_query = loader.load(query)

        if result_from_query.empty:
            break

        active_user_list = pd.concat([active_user_list, result_from_query], ignore_index=True)

        if len(active_user_list) < offset:
            break

        offset += limit
        
    return active_user_list


def build_query(parsed_execution_time_mills, execution_time_mills_a_day_ago, limit, offset):
    return f"""SET useMultistageEngine=true;
            Select userId, appId, serviceName,
            jsonextractscalar(eventPropertiesJson, '$.component', 'STRING') component,
            jsonextractscalar(eventPropertiesJson, '$.item', 'STRING') item,
            count(jsonextractscalar(eventPropertiesJson, '$.component', 'STRING')) component_count,
            count(jsonextractscalar(eventPropertiesJson, '$.item', 'STRING')) item_count
            from hamropatro_analytics where eventTimeStamp >= {execution_time_mills_a_day_ago} and eventTimeStamp < {parsed_execution_time_mills}
            GROUP BY userId, appId, serviceName, component, item limit {limit} OFFSET {offset};"""  # Specify your SQL query here


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
