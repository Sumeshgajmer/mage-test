from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra.policies import DCAwareRoundRobinPolicy

auth_provider = PlainTextAuthProvider(username='hp_user', password='v6dhzh5dvptn')
protocol_version = 4
load_balancing_policy = DCAwareRoundRobinPolicy(local_dc='datacenter1')
cluster = Cluster(contact_points=['108.181.92.136'], port=9042, auth_provider=auth_provider,
            protocol_version=4, load_balancing_policy=load_balancing_policy)
session = cluster.connect()
session.set_keyspace('hamropatro_analytics')
table_name = 'active_users'

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
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
        UPDATE hamropatro_analytics.active_users set last_updated_timestamp =  ? ,
        last_active_device_type =  ? ,last_active_device_os =  ? ,
        last_active_device_id =  ? WHERE app_id =  ? and service_name =  ?
        and feature =  ? and user_id =  ?
    """
    
    prepare_stmt = session.prepare(insert_query)
    batch_stmt = batch_insert_data(prepare_stmt, data)

    session.execute(batch_stmt)

# Function to perform batch insert
def batch_insert_data(prepared_stmt, batch_data):
    batch_stmt = BatchStatement()

    for index, row in batch_data.iterrows():

        batch_stmt.add(prepared_stmt, (row.eventTimeStamp, row.deviceType, row.os,
                            row.deviceId, row.appId, row.serviceName, row.feature, row.userId))
        return batch_stmt
        # Add each insert statement to the batch
