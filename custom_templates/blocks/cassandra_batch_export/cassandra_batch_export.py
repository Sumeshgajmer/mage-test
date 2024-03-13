from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra.policies import DCAwareRoundRobinPolicy


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

keyspcae_name = 'keyspcae_name'
table_name = 'table_name'

@data_exporter
def export_data(data, *args, **kwargs):
    session = initializeDatabase(**kwargs)
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    insert_query = f""" yout query to execute for data export"""
    
    prepare_stmt = session.prepare(insert_query)
    batch_stmt = batch_insert_data(prepare_stmt, data)

    session.execute(batch_stmt)

def initializeDatabase(**kwargs):
    # specify this in the vairable sections
    user_name = kwargs.get('cassandra_user_name')
    password = kwargs.get('cassandra_password')
    protocol_version = kwargs.get('protocol_version')
    local_dc = kwargs.get('cassandra_dc')
    contact_points = kwargs.get('cassandra_contact_point');
    port = kwargs.get('cassandra_port')

    auth_provider = PlainTextAuthProvider(username= user_name, password = password)
    load_balancing_policy = DCAwareRoundRobinPolicy(local_dc=local_dc)

    cluster = Cluster(contact_points=[contact_points], port=port, auth_provider=auth_provider, protocol_version= protocol_version,
                    load_balancing_policy=load_balancing_policy)

    return cluster.connect()
    

# Function to perform batch insert
def batch_insert_data(prepared_stmt, batch_data):
    batch_stmt = BatchStatement()

    for index, row in batch_data.iterrows():

        batch_stmt.add(prepared_stmt, (row.eventTimeStamp, row.deviceType, row.os,
                            row.deviceId, row.appId, row.serviceName, row.feature, row.userId))
        return batch_stmt
        # Add each insert statement to the batch


