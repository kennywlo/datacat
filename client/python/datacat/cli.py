#!/usr/bin/python

import sys
import pprint
import argparse
import collections

from model import DatacatNode, unpack
from client import Client, DcException
from config import *

def build_argparser():
    parser = argparse.ArgumentParser(description="Python CLI for Data Catalog RESTful interfaces")
    parser.add_argument('-U', '--base-url', help="Override base URL for client", action="store")
    parser.add_argument('-D', '--experiment', "--domain", help="Set experiment domain for requests", default="srs")
    parser.add_argument('-M', '--mode', help="Set server mode", choices=("dev","prod"), default="prod")
    parser.add_argument('-f', '--format', dest="accept", default="json", help="Default format is JSON. JSON will attempted to be processed further")
    parser.add_argument('-r', '--show-request', action="store_true", dest="show_request",
                        help="Show request URL", default=False)
    parser.add_argument('-R', '--show-response', action="store_true", dest="show_response",
                        help="Attempt to show formatted response", default=False)
    parser.add_argument('-Rw', '--show-raw-response', action="store_true", dest="show_raw_response",
                        help="Show raw response", default=False)
    parser.add_argument('-rH', '--show-request-headers', action="store_true", dest="request_headers",
                        help="Show HTTP headers", default=False)
    parser.add_argument('-RH', '--show-response-headers', action="store_true", dest="response_headers",
                        help="Show HTTP headers", default=False)
    subparsers = parser.add_subparsers(help="Command help")
    
    def add_search(subparsers):
        parser_search = subparsers.add_parser("search", help="Search command help",
                                              formatter_class=argparse.RawTextHelpFormatter)
        parser_search.add_argument('path', help="Container Search path (or pattern)")
        parser_search.add_argument('-v', '--version', dest="versionId",
                                   help="Version to query (default equivalent to 'current' for latest version)")
        parser_search.add_argument('-s', '--site', dest="site",
                                   help="Site to query (default equivalent to 'canonical' for master site)")
        parser_search.add_argument('-q', '--query', dest="query", help="Query String for datasets")
        parser_search.add_argument('--show', nargs="*", metavar="FIELD", help="List of columns to return")
        parser_search.add_argument('--sort', nargs="*", metavar="FIELD", help=
        "Fields and metadata to sort by. \nIf sorting in descending order, \nappend a dash to the end of the field. " +
        "\n\nExamples: \n --sort nRun- nEvents\n --sort nEvents+ nRun-")
        parser_search.set_defaults(command="search")
    
    def add_path(subparsers):
        cmd = "path"
        parser_path = subparsers.add_parser(cmd, help="search help")
        parser_path.add_argument('path', help="Path to stat")
        parser_path.add_argument('-v', '--version', dest="versionId",
                                 help="Version to query (default equivalent to 'current' for latest version)")
        parser_path.add_argument('-s', '--site', dest="site",
                                 help="Site to query (default equivalent to 'canonical' for master site)")
        parser_path.add_argument('-S', '--stat', dest="stat",
                                 help="Type of stat to return (for containers)", choices=("none","basic","dataset"))
        parser_path.set_defaults(command=cmd)
    
    def add_children(subparsers):
        cmd = "children"
        parser_children = subparsers.add_parser(cmd, help="Help with the children command")
        parser_children.add_argument('path', help="Container to query")
        parser_children.add_argument('-v', '--version', dest="versionId",
                                     help="Version to query (default equivalent to 'current' for latest version)")
        parser_children.add_argument('-s', '--site', dest="site",
                                     help="Site to query (default equivalent to 'canonical' for master site)")
        parser_children.add_argument('-S', '--stat', dest="stat",
                                 help="Type of stat to return", choices=("none","basic","dataset"))
        parser_children.set_defaults(command=cmd)

    def add_mkds(subparsers):
        cmd = "mkds"
        parser_children = subparsers.add_parser(cmd, help="Making a dataset")
        parser_children.add_argument('path', help="Dataset path")
        parser_children.add_argument('name', help="Dataset name")
        parser_children.add_argument('dataType', help="Dataset data type")
        parser_children.add_argument('fileFormat', help="Dataset file format")
        parser_children.add_argument('versionId',
                                     help="Version to query (default equivalent to 'current' for latest version)")
        parser_children.add_argument('site', help="Location site")
        parser_children.add_argument('resource', help="Location resource")
        parser_children.set_defaults(command=cmd)

    def add_rmds(subparsers):
        cmd = "rmds"
        parser_children = subparsers.add_parser(cmd, help="Help with the children command")
        parser_children.add_argument('path', help="Path of dataset to remove")
        parser_children.set_defaults(command=cmd)

    def add_mkdir(subparsers):
        cmd = "mkdir"
        parser_children = subparsers.add_parser(cmd, help="Help with the children command")
        parser_children.add_argument('path', help="Container path")
        parser_children.add_argument('type', help="Container Type (defaults to folder)", choices=("folder","group"))
        parser_children.set_defaults(command=cmd)

    def add_rmdir(subparsers):
        cmd = "rmdir"
        parser_children = subparsers.add_parser(cmd, help="Remove a container (group or folder)")
        parser_children.add_argument('path', help="Path of container to remove")
        parser_children.add_argument('type', help="Container Type (defaults to folder)", choices=("folder","group"))
        parser_children.set_defaults(command=cmd)

    add_path(subparsers)
    add_children(subparsers)
    add_search(subparsers)
    add_mkds(subparsers)
    add_rmds(subparsers)
    add_mkdir(subparsers)
    add_rmdir(subparsers)
    
    return parser


def main():
    parser = build_argparser()
    args, extra = parser.parse_known_args()

    command = args.command
    target = args.__dict__.pop("path")
    params = args.__dict__

    base_url = args.base_url if hasattr(args, 'base_url') and args.base_url is not None \
        else CONFIG_URL(args.experiment, args.mode)
    client = Client(base_url)
    client_method = getattr(client, command)

    resp = None
    try:
        if len(params) > 0:
            resp = client_method(target, **params)
        else:
            resp = client_method(target)
    except DcException as error:
        if hasattr(error, "message"):
            print("Error occurred:\nMessage: %s" %(error.message))
            if hasattr(error, "type"):
                print("Type: %s" %(error.type))
            if hasattr(error, "cause"):
                print("Cause: %s" %(error.cause))
        else:
            # Should have content
            print(error.content)
        sys.exit(1)

    pp = pprint.PrettyPrinter(indent=2)

    if(args.accept != 'json'):
        sys.stderr.write("Response: %d\n" %(resp.status_code))
        if(args.accept == 'xml'):
            from xml.dom.minidom import parseString
            xml= parseString(resp.content)
            print(xml.toprettyxml())
        if(args.accept == 'txt'):
            print(resp.text)
        sys.exit(1)

    if(resp.status_code == 204):
        print("No Content")
        sys.exit(1)

    retObjects = unpack(resp.content)

    if args.show_response:
        print("Object Response:")
        show = retObjects if isinstance(retObjects, collections.Iterable) else [retObjects]
        pp.pprint([i.__dict__ if isinstance(i, DatacatNode) else i for i in show])

    if command == "search":
        def print_search_info(datasets, metanames):
            print("\nListing locations...")
            print( "Resource\tPath\t%s" %("\t".join(metanames)))
            for dataset in datasets:
                extra = ""
                if hasattr(dataset, "metadata"):
                    extra = "\t".join([str(dataset.metadata.get(i)) for i in metanames])
                if hasattr(dataset, "resource"):
                    print( "%s\t%s\t%s" %(dataset.resource, dataset.path, extra))
                elif hasattr(dataset, "locations"):
                    for location in dataset.locations:
                        print( "%s\t%s\t%s" %(location.resource, dataset.path, extra))

        metanames = []
        if args.show is not None:
            metanames.extend(args.show)
        if args.sort is not None:
            s = []
            s.extend(args.sort)
            for i in s:
                if i[-1] in ("+", "-"):
                    metanames.append(i[0:-1])
                else:
                    metanames.append(i)
        metanames = set(metanames)
        print_search_info(retObjects, metanames)


if __name__ == '__main__':
    main()
