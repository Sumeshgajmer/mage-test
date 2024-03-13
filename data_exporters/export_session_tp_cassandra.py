from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra.policies import DCAwareRoundRobinPolicy


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

keyspcae_name = 'hamropatro_analytics'
table_name = 'active_users'

@data_exporter
def export_data(data, *args, **kwargs):
    cluster = initializeDatabase(**kwargs)
    session = cluster.connect()

    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    insert_query = f"""
        UPDATE {keyspcae_name}.{table_name} set last_updated_timestamp =  ? ,
        last_active_device_type =  ? ,last_active_device_os =  ? ,
        last_active_device_id =  ? WHERE app_id =  ? and service_name =  ?
        and feature =  ? and user_id =  ?
    """
    
    prepare_stmt = session.prepare(insert_query)
    batch_stmt = batch_insert_data(prepare_stmt, data)

    session.execute(batch_stmt)
    
    session.shutdown()
    cluster.shutdown()  

def initializeDatabase(**kwargs):
    user_name = kwargs.get('cassandra_user_name')
    password = kwargs.get('cassandra_password')
    protocol_version = kwargs.get('protocol_version')
    local_dc = kwargs.get('cassandra_dc')
    contact_points = kwargs.get('cassandra_contact_point');
    port = kwargs.get('cassandra_port')

    auth_provider = PlainTextAuthProvider(username= user_name, password = password)
    load_balancing_policy = DCAwareRoundRobinPolicy(local_dc=local_dc)

    return Cluster(contact_points=[contact_points], port=port, auth_provider=auth_provider, protocol_version= protocol_version,
                    load_balancing_policy=load_balancing_policy)    

# Function to perform batch insert
def batch_insert_data(prepared_stmt, batch_data):
    batch_stmt = BatchStatement()

    for index, row in batch_data.iterrows():

        batch_stmt.add(prepared_stmt, (row.eventTimeStamp, row.deviceType, row.os,
                            row.deviceId, row.appId, row.serviceName, row.feature, row.userId))
        return batch_stmt
        # Add each insert statement to the batch


