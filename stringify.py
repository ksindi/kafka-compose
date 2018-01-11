"""Convert a formatted JSON file into a string for use in a REST call"""
import json
import argparse


def stringify(d):
    jsonstr = json.dumps(d, separators=(',', ':'))
    return jsonstr.replace('"', '\\"').replace('\n', '\\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('jsonfile', metavar='FILE', help='JSON file')
    args = parser.parse_args()

    with open(args.jsonfile) as f:
        d = json.load(f)

    print(stringify(d))
