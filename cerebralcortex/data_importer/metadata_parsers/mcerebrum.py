# Copyright (c) 2019, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from cerebralcortex.core.metadata_manager.stream import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.data_importer.util.helper_methods import rename_column_name


def mcerebrum_metadata_parser(metadata: dict) -> dict:
    """
    Convert mcerebrum old metadata format to CC-kernel version 3.x metadata format

    Args:
        metadata (dict): mcerebrum old metadata format

    Returns:
        dict: {"platform_metadata":platform_metadata, "stream_metadata":metadata}
    """

    # check stream name; get correct metadata if metadata exist in mysql
    annotation_name = None
    platform_metadata = get_platform_metadata(metadata)
    if platform_metadata:
        annotation_name = platform_metadata.name
    metadata = convert_json_to_metadata_obj(metadata, annotation_name)
    return {"platform_metadata": platform_metadata, "stream_metadata": metadata}


def new_data_descript_frmt(data_descriptor: dict) -> dict:
    """
    convert old mcerebrum data descriptor format to CC-kernel 3.x format

    Args:
        data_descriptor (dict): old mcerebrum data descriptor format

    Returns:
        dict: {"name":"..", "type:"..", "attributes":{...}....}
    """
    basic_dd = {}
    attr = {}
    if len(data_descriptor) == 0:
        return {}
    for key, value in data_descriptor.items():
        if key == "data_type" or key=="type":
            basic_dd["type"] = value
        elif key == "name":
            basic_dd[key] = rename_column_name(value)
        else:
            attr[key] = value

    # remove any name inside attribute to avoid confusion
    if "name" in attr:
        attr.pop("name")
    if "data_type" in attr:
        attr.pop("data_type")
    new_data_descriptor = basic_dd
    if len(new_data_descriptor) > 0:
        new_data_descriptor["attributes"] = attr
    return new_data_descriptor


def get_platform_metadata(metadata: dict) -> Metadata:
    """
    Build platform metadata out of old mcerebrum metadata format.

    Args:
        metadata (dict): old mecerebrum metadata

    Returns:
        Metadata: Metadata class object
    """
    stream_name = metadata.get("name", "name_not_available")
    execution_context = metadata.get("execution_context")
    platform_metadata = execution_context.get("platform_metadata", {})  # dict
    application_metadata = execution_context["application_metadata"]  # dict
    wrist = ""
    if platform_metadata.get("name", "") != "":
        if "left" in stream_name:
            wrist = "_left_wrist"
        elif "right" in stream_name:
            wrist = "_right_wrist"
        elif "sleep" in stream_name:
            wrist = "_sleep_wrist"
        stream_name = platform_metadata.get("name", "name_not_available") + wrist + "_platform_annotation"
        return Metadata().set_name(stream_name).set_version(1). \
            set_description(application_metadata.get("description", "no description available.")).add_dataDescriptor(
            DataDescriptor().set_name("device_info").set_type("dict").set_attribute("description",
                                                                                    "Platform information, e.g., {'name': 'motionsensehrv', 'device_id': 'c1:c0'}. device_id is optional.")
        ).add_module(ModuleMetadata().set_name(application_metadata.get("name", "name_not_available")).set_version(
            application_metadata.get("version", 1)).set_attribute("description", application_metadata.get("description",
                                                                                                          "no description available.")).set_author(
            "Monowar Hossain", "smhssain@memphis.edu"))
    else:
        return None


def new_module_metadata(ec: dict) -> dict:
    """
    convert old mcerebrum data execution_context format to CC-kernel 3.x format

    Args:
        ec (dict): old mcerebrum execution_context block

    Returns:
        dict: {"name":".....}
    """

    new_module = {}
    nm_attr = {}
    application_metadata = ec["application_metadata"]  # dict
    algorithm = ec["processing_module"]["algorithm"]  # list of dict
    processing_module = ec["processing_module"]

    for key, value in application_metadata.items():
        if key == "version_name":
            new_module["version"] = value
        elif key == "name":
            new_module[key] = value
        else:
            nm_attr[key] = value

    for key, value in processing_module.items():
        if key != "algorithm" and key != "input_streams":
            nm_attr[key] = value

    for tmp in algorithm:
        for key, value in tmp.items():
            if key == "authors":
                new_module["authors"] = value
            elif key == "reference":
                if tmp.get("reference", None) is not None:
                    for key, value in tmp.get("reference", {}).items():
                        nm_attr[key] = value
            else:
                nm_attr[key] = value

    if "name" in nm_attr:
        nm_attr.pop("name")
    new_module["attributes"] = nm_attr

    return new_module


def convert_json_to_metadata_obj(metadata: dict, annotation_name: str) -> Metadata:
    """
    Convert old mcerebrum metadata json files in to new CC-kernel 3.x compatible format

    Args:
        metadata (dict): mcerebrum old metadata format
        annotation_name (str): name of annotation stream

    Returns:
        Metadata object

    """
    new_metadata = {}
    new_dd_list = []
    annotations = []
    new_module = []

    if isinstance(metadata["data_descriptor"], dict):
        new_dd = new_data_descript_frmt(metadata["data_descriptor"])
        if new_dd:
            new_dd_list.append(new_dd)
    else:
        for dd in metadata["data_descriptor"]:
            new_dd = new_data_descript_frmt(dd)
            if new_dd:
                new_dd_list.append(new_dd)
    new_module.append(new_module_metadata(metadata["execution_context"]))

    if annotation_name and annotation_name is not None:
        annotations.append(annotation_name)

    input_streams = []
    if "input_streams" in metadata["execution_context"]["processing_module"]:
        for input_stream in metadata["execution_context"]["processing_module"]["input_streams"]:
            input_streams.append(input_stream["name"])

    new_metadata["name"] = metadata["name"]
    new_metadata["description"] = metadata.get("description", "no-description")
    new_metadata["annotations"] = annotations
    new_metadata["input_streams"] = input_streams
    new_metadata["data_descriptor"] = new_dd_list
    new_metadata["modules"] = new_module

    return Metadata().from_json_file(new_metadata)
