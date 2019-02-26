import json
import os
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from cerebralcortex import Kernel
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.data_manager.sql.data import SqlData


def metadata_parser(parser, metadata, df):
    return parser(metadata, df)

def parse_mcerebrum_metadata(metadata, df):
    platform_metadata = get_platform_metadata(metadata)
    metadata = convert_json_to_metadata_obj(metadata, platform_metadata.name, df)
    return {"platform_metadata":platform_metadata, "stream_metadata":metadata}


def new_data_descript_frmt(data_descriptor, df=None):
    new_data_descriptor = {}
    basic_dd = {}
    attr = {}
    if df is not None:
        column_names = df.columns
        column_types = df.dtypes
        for col_name, col_type in zip(column_names, column_types):
            if col_name not in ["timestamp", "localtime", "user", "version"]:
                basic_dd["name"] = col_name
                basic_dd["type"]= str(col_type)
    for key, value in data_descriptor.items():
        if key=="name" or key=="type":
            pass
        else:
            attr[key] = value
    # remove any name inside attribute to avoid confusion
    if "name" in attr:
        attr.pop("name")
    if "data_type" in attr:
        attr.pop("data_type")
    new_data_descriptor["data_descriptor"] = basic_dd
    new_data_descriptor["data_descriptor"]["attributes"] =  attr
    sd = basic_dd
    sd["attributes"] = attr
    return sd

def get_platform_metadata(metadata):

    stream_name = metadata.get("name", "name_not_available")
    execution_context = metadata.get("execution_context")
    platform_metadata = execution_context["platform_metadata"] #dict
    application_metadata = execution_context["application_metadata"] #dict

    if platform_metadata.get("device_id", "")!="":
        stream_name = stream_name+"_"+platform_metadata.get("name", "name_not_available")+"_"+platform_metadata.get("device_id", "")
    else:
        stream_name = stream_name+"_"+platform_metadata.get("name", "name_not_available")

    return Metadata().set_name(stream_name).set_version(1). \
        set_description(application_metadata.get("description", "no description available.")).add_dataDescriptor(
        DataDescriptor().set_name("device_id").set_type("string").set_attribute("description", "hardware device id.")
    ).add_module(ModuleMetadata().set_name(application_metadata.get("name", "name_not_available")).set_version(application_metadata.get("version", 1)).set_attribute("description", application_metadata.get("description", "no description available.")).set_author(
        "Monowar Hossain", "smhssain@memphis.edu"))


def new_module_metadata(ec_algo_pm):
    new_module = {}
    nm_attr = {}
    ec = ec_algo_pm
    application_metadata = ec["application_metadata"] #dict
    algorithm = ec["processing_module"]["algorithm"] # list of dict
    processing_module = ec["processing_module"]


    for key, value in application_metadata.items():
        if key=="version_name":
            new_module["version"] = value
        elif key=="name":
            new_module[key] = value
        else:
            nm_attr[key] = value

    for key, value in processing_module.items():
        if key!="algorithm" and key!="input_streams":
            nm_attr[key] = value

    for tmp in algorithm:
        for key, value in tmp.items():
            if key=="authors":
                new_module["authors"] = value
            elif  key=="reference":
                if tmp.get("reference", None) is not None:
                    for key, value in tmp.get("reference", {}).items():
                        nm_attr[key] = value
            else:
                nm_attr[key] = value

    if "name" in nm_attr:
        nm_attr.pop("name")
    new_module["attributes"] = nm_attr

    return new_module

def convert_json_to_metadata_obj(metadata, annotation_name, df):
    new_metadata = {}
    new_dd_list = []
    annotations = []
    new_module = []
    if isinstance(metadata["data_descriptor"],dict):
        new_dd_list.append(new_data_descript_frmt(metadata["data_descriptor"], df))
    else:
        for dd in metadata["data_descriptor"]:
            new_dd_list.append(new_data_descript_frmt(dd, df))

    new_module.append(new_module_metadata(metadata["execution_context"]))

    annotations.append(annotation_name)

    input_streams = []
    if "input_streams" in metadata["execution_context"]["processing_module"]:
        for input_stream in metadata["execution_context"]["processing_module"]["input_streams"]:
            input_streams.append(input_stream["name"])

    new_metadata["name"] = metadata["name"]
    new_metadata["description"] = metadata.get("description", "xxxx")
    new_metadata["annotations"] = annotations
    new_metadata["input_streams"] = input_streams
    new_metadata["data_descriptor"] = new_dd_list
    new_metadata["modules"] = new_module

    return Metadata().from_json_file(new_metadata)