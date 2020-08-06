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

import json
import os
import uuid
from typing import List

from cerebralcortex.core.metadata_manager.stream.data_descriptor import DataDescriptor
from cerebralcortex.core.metadata_manager.stream.module_info import ModuleMetadata


class Metadata():
    def __init__(self):
        """
        Constructor

        """
        self.name = None
        self.description = ""
        self.study_name = os.getenv("STUDY_NAME")
        self.metadata_hash = None
        self.input_streams = []
        self.annotations = []
        self.data_descriptor = []
        self.modules = []

    def set_name(self, value:str):
        """
        set name of a stream

        Args:
            value (str): name of a stream

        Returns:
            self

        """
        self.name = value
        return self

    def set_study_name(self, value:str):
        """
        set study name

        Args:
            value (str): study name

        Returns:
            self

        """
        self.study_name = value
        return self

    def get_name(self):
        """

        Returns: name of a stream

        """
        return self.name

    def set_description(self, stream_description:str):
        """
        Add stream description

        Args:
            stream_description (str): textual description of a stream

        Returns:
            self

        """
        self.description = stream_description
        return self

    def add_dataDescriptor(self, dd: DataDescriptor):
        """
        Add data description of a stream

        Args:
            dd (DataDescriptor): data descriptor

        Returns:
            self

        """
        self.data_descriptor.append(dd)
        return self

    def get_dataDescriptor(self, name):
        """
        get data descriptor by name

        Args:
            name (str):

        Returns:
            DataDescriptor object
        """
        for dd in self.data_descriptor:
            if dd.name == name:
                return dd

    def add_input_stream(self, input_stream:str):
        """
        Add input streams that were used to derive a new stream

        Args:
            input_stream (str): name of input stream OR list of input_stream names

        Returns:
            self
        """
        if isinstance(input_stream,list):
            self.input_streams = input_stream
        else:
            self.input_streams.append(input_stream)
        return self

    def add_annotation(self, annotation:str):
        """
        Add annotation stream name

        Args:
            annotation (str): name of annotation  or list of strings

        Returns:
            self
        """
        if isinstance(annotation, list):
            self.annotations = annotation
        else:
            self.annotations.append(annotation)
        return self

    def add_module(self, mod: ModuleMetadata):
        """
        Add module metadata

        Args:
            mod (ModuleMetadata): module metadata

        Returns:
            self
        """
        self.modules.append(mod)
        return self

    def is_valid(self)->bool:
        """
        check whether all required fields are set

        Returns:
            bool: True if fields are set or throws an exception in case of missing values
        Exception:
            ValueError: if metadata fields are not set

        """

        if not self.name:
            raise ValueError("Stream name is not defined.")
        if not self.study_name:
            raise ValueError("Study name is missing. use metadata.set_study_name method to set study name.")
        if not self.description:
            raise ValueError("Stream description is not defined.")
        if len(self.data_descriptor)==0:
            raise Exception("Data descriptor length cannot be 0.")

        # for dd_obj in self.data_descriptor:
        #     if (dd_obj.attributes is None or len(dd_obj.attributes)==0):
        #         raise ValueError("Add brief description for each column in data desciptor. For example, DataDescriptor().set_attribute('description'', 'sleep time''))")
        for mm_obj in self.modules:
            if (mm_obj.name is None or mm_obj.name==""):
                raise ValueError("Module name and/or version fields are missing in module info.")
            if len(mm_obj.authors)==0:
                raise ValueError("Author information is missing.")
        return True

    def to_json(self)->dict:
        """
        Convert MetaData object into a dict (json) object

        Returns:
            dict: dict form of MetaData object
        """

        data_descriptor = []
        module_metadata = []
        metadata_json = {}
        for dd_obj in self.data_descriptor:
            data_descriptor.append(dd_obj.__dict__)
        for mm_obj in self.modules:
            module_metadata.append(mm_obj.__dict__)
        metadata_json["name"] = self.name
        metadata_json["study_name"] = self.study_name
        metadata_json["description"] = self.description
        metadata_json["annotations"] = self.annotations
        metadata_json["input_streams"] = self.input_streams
        metadata_json["data_descriptor"] = data_descriptor
        metadata_json["modules"] = module_metadata
        return metadata_json

    def get_hash(self)->str:
        """
        Get the unique hash of metadata. Hash is generated based on "stream-name + data_descriptor + module-metadata"

        Returns:
            str: hash id of metadata

        """
        name = self.name
        study_name = self.study_name
        data_descriptor = ""
        modules = ""
        for dd in self.data_descriptor:
            data_descriptor += str(dd.name)+str(dd.type)
        for mm in self.modules:
            modules += str(mm.name) + str(mm.version) + str(mm.authors)
        hash_string = str(study_name)+str(name)+"None"+str(data_descriptor)+str(modules)
        hash_string = hash_string.strip().lower().replace(" ", "")

        return str(uuid.uuid3(uuid.NAMESPACE_DNS, hash_string))

    def get_hash_by_json(self, metadata:dict=None)->str:
        """
        Get the unique hash of metadata. Hash is generated based on "stream-name + data_descriptor + module-metadata"

        Args:
            metadata: only pass this if this method is used on a dict object outside of Metadata class
        Returns:
            str: hash id of metadata

        """

        name = metadata.get("name")
        data_descriptor = ""
        modules = ""
        for dd in metadata.get("data_descriptor"):
            data_descriptor += str(dd.get("name"))+str(dd.get("type"))
        for mm in metadata.get("modules"):
            modules += str(mm.get("name")) + str(mm.get("version")) + str(mm.get("authors"))
        hash_string = str(name)+"None"+str(data_descriptor)+str(modules)
        hash_string = hash_string.strip().lower().replace(" ", "")

        return str(uuid.uuid3(uuid.NAMESPACE_DNS, hash_string))


    def from_json_sql(self, metadata_json: dict)->List:
        """
        Convert dict (json) objects into Metadata class objects

        Args:
            json_list dict: metadata dict

        Returns:
            Metadata: metadata class object

        """
        data_descriptor_list = []
        module_list = []

        if isinstance(metadata_json, str):
            metadata_json = json.loads(metadata_json)

        md = Metadata()
        if isinstance(metadata_json, dict):
            metadata = json.loads(metadata_json.get("metadata"))
            data_descriptors = metadata["data_descriptor"]
            module_info = metadata["modules"]

            if not isinstance(data_descriptor_list, list):
                raise ValueError("data_descriptor field must be a list of data descriptors.")
            if not isinstance(module_info, list):
                raise ValueError("modules field must be a type of list.")

            for dd in data_descriptors:
                data_descriptor_list.append(DataDescriptor().from_json(dd))

            for mm in module_info:
                module_list.append(ModuleMetadata().from_json(mm))

            md.data_descriptor = data_descriptor_list
            md.modules = module_list
            md.name = metadata_json["name"]
            md.description = metadata.get("description", "")
            md.version = int(metadata_json["version"])
            md.input_streams = metadata.get("input_streams", [])
            md.annotations = metadata.get("annotations", [])
            md.metadata_hash = metadata_json["metadata_hash"]
        return md

    def from_json_file(self, metadata: dict)->List:
        """
        Convert dict (json) objects into Metadata class objects

        Args:
            json_list dict: metadata dict

        Returns:
            Metadata: metadata class object

        """
        data_descriptor_list = []
        module_list = []
        
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        metadata = json.dumps(metadata)
        metadata = json.loads(metadata.lower())
        
        md = Metadata()
        if isinstance(metadata, dict):
            data_descriptors = metadata["data_descriptor"]
            module_info = metadata["modules"]

            if not isinstance(data_descriptor_list, list):
                raise ValueError("data_descriptor field must be a list of data descriptors.")
            if not isinstance(module_info, list):
                raise ValueError("modules field must be a type of list.")

            for dd in data_descriptors:
                data_descriptor_list.append(DataDescriptor().from_json(dd))

            for mm in module_info:
                module_list.append(ModuleMetadata().from_json(mm))

            md.data_descriptor = data_descriptor_list
            md.modules = module_list
            md.name = metadata.get("name", "")
            md.study_name = metadata.get("study_name","")
            md.description = metadata.get("description", "")
            md.version = int(metadata.get("version", 1))
            md.input_streams = metadata.get("input_streams", [])
            md.annotations = metadata.get("annotations", [])
            md.metadata_hash = metadata.get("metadata_hash", "no-hash")
        return md

    def __repr__(self):
        data = self.to_json()
        return json.dumps(data, indent=4, sort_keys=True)
        #return str(self.__dict__)

    ###################################### Overridden Python methods ##########################################
    # def __str__(self):
    #     pass