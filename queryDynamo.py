import boto3

dynamoDB = boto3.resource('dynamodb')

table = dynamoDB.Table('agent-purchase')


def get_table_metadata(table_name):
    """
    Get some metadata about chosen table.
    """
    table = dynamoDB.Table(table_name)

    return {
        'num_items': table.item_count,
        'primary_key_name': table.key_schema[0],
        'status': table.table_status,
        'bytes_size': table.table_size_bytes,
        'global_secondary_indices': table.global_secondary_indexes
    }


get_table_metadata("agent-purchase")
