import yaml
import pyodbc
import struct
from itertools import chain, repeat
from collections import OrderedDict
from azure.identity import AzureCliCredential

def read_yaml_file(filename):
    with open(filename, 'r') as file:
        data = yaml.safe_load(file)
    return data

def ordered_yaml_dump(data, stream=None, **kwds):
    class OrderedDumper(yaml.SafeDumper):
        pass
    def _dict_representer(dumper, data):
        return dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())
    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    return yaml.dump(data, stream, OrderedDumper, **kwds)

def write_yaml_file(filename, data):
    with open(filename, 'w') as file:
        ordered_yaml_dump(data, file)
        file.flush()

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

data = read_yaml_file('d365_tables.yml')

print("Source Lakehouse: ", data['source_lakehouse'])
print("Tables: ", data['tables'])

# Establish a connection to the Microsoft Fabric Lakehouse
credential = AzureCliCredential()
sql_endpoint = "b27nglr6dgderlhlbqidfj2kge-uf6n4xvquwoejm7ep7blowjgei.datawarehouse.fabric.microsoft.com"
database = data['source_lakehouse']
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_endpoint},1433;Database=f{database};Encrypt=Yes;TrustServerCertificate=No"

# prepare the access token
token_object = credential.get_token("https://database.windows.net//.default")
token_as_bytes = bytes(token_object.token, "UTF-8")
encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
attrs_before = {1256: token_bytes}

# build the connection
connection = pyodbc.connect(connection_string, attrs_before=attrs_before)

# Initialize the template data structure
template_data = OrderedDict([
    ("version", 2),
    ("sources", [
        OrderedDict([
            ("name", "dataverse"),
            ("database", data['source_lakehouse']),
            ("description", "D365 external table sources for ods"),
            ("tables", [])
        ])
    ])
])

for table in data['tables']:
    query = f"SELECT COLUMN_NAME, DATA_TYPE FROM {data['source_lakehouse']}.[INFORMATION_SCHEMA].[COLUMNS] WHERE TABLE_NAME = '{table}'"
    result = execute_query(connection, query)
    print(result)

    # Initialize the table data structure
    table_data = OrderedDict([("name", table), ("columns", [])])

    for row in result:
        # Append the column data to the table data structure
        table_data["columns"].append(OrderedDict([("name", row[0]), ("data_type", row[1])]))
    
    # Append the table data to the template data
    template_data["sources"][0]["tables"].append(table_data)

# Write the template data to the sources.yml file
write_yaml_file('sources.yml', template_data)

# Print a success message
print("The template data was successfully written to the sources.yml file.")
