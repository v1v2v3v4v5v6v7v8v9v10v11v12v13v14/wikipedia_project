from typing import Optional, Dict
from fastavro import schemaless_writer, schemaless_reader
import json
import io
from config.settings import AVRO_SCHEMA_PATH

def load_schema(file_path:str)->Dict:
    '''Load an avro schema from a given filepath '''
    with open(file_path, 'r') as f:
        return json.load(AVRO_SCHEMA_PATH)

def serialize_record(schema: Dict, record_data: Dict)->bytes:
    '''
    Serializes record into avro format using the schema
    '''
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, record_data)
    return buffer.getvalue()

def dserialize_record(record_data: bytes) -> Dict:
    '''
    Deserializes avro bytes record_data back into a dictionary using the provided schema
    '''
    buffer=io.BytesIO(record_data)
    record = schemaless_reader(buffer)
    return record



