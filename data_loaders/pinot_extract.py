from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.pinot import Pinot
from os import path
from datetime import datetime
from dateutil import parser
import pandas as pd

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
        return get_paginated_active_users(loader, parsed_execution_time_mills, execution_time_mills_a_day_ago)
    

def get_paginated_active_users(loader, parsed_execution_time_mills, execution_time_mills_a_day_ago):
    offset = 0
    limit = 100000
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
    return f"""Select appId,
                   feature,
                   serviceName,
                   userId,
                   eventTimeStamp,
                   deviceType,
                   os,
                   deviceId from active_user 
                   where eventTimeStamp >= {execution_time_mills_a_day_ago} and eventTimeStamp < {parsed_execution_time_mills}
               order by eventTimeStamp desc limit {limit} offset {offset}"""  # Specify your SQL query here


def get_time_millis_from_date(date, date_pattern):
    try:
        # Parse the input date string using the specified pattern
        parsed_date = datetime.strptime(date, date_pattern)
        # Convert the parsed date to milliseconds since the epoch
        return int(parsed_date.timestamp() * 1000)
    except Exception as e:
        raise RuntimeError(f"Unable to format the date {date}")


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
