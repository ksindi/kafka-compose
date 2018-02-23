"""Convert a JSON data file a JSON format that AVRO understands.

Deals with typical case here:
https://stackoverflow.com/questions/27485580/how-to-fix-expected-start-union-got-value-number-int-when-converting-json-to-av

Hopefully compatability will eventually be out of the box:
https://issues.apache.org/jira/browse/AVRO-1582
"""
import json
import argparse

import six
import avro.schema
from avro_json_serializer import AvroJsonSerializer
if six.PY2:
    from avro.schema import make_avsc_object as SchemaFromJSONData
else:
    from avro.schema import SchemaFromJSONData


def serialize(schema, data):
    avro_schema = SchemaFromJSONData(schema, avro.schema.Names())
    serializer = AvroJsonSerializer(avro_schema)
    return serializer.to_json(data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('schema', metavar='FILE', help='AVRO schema')
    parser.add_argument('jsonfile', metavar='FILE', help='JSON file')
    args = parser.parse_args()

    with open(args.schema) as f_schema, open(args.jsonfile) as f_data:
        schema = json.load(f_schema)
        data = json.load(f_data)

    print(serialize(schema, data))
