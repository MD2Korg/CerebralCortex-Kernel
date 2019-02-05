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
import uuid
from typing import List

from cerebralcortex.core.metadata_manager.stream.data_descriptor import DataDescriptor
from cerebralcortex.core.metadata_manager.stream.module_info import ModuleMetadata


class Metadata():
    def __init__(self):
        """
        Constructor

        """
        self._name = None
        self._version = None
        self._description = "",
        self._metadata_hash = None
        self._dataDescriptor = []
        self._module = []

    @property
    def name(self)->str:
        """
        get stream name

        Returns:
            str: name

        """
        return self._name

    @name.setter
    def name(self, value:str):
        """
        set stream name

        Args:
            value (str): name
        """
        self._name = value

    @property
    def version(self)->int:
        """
        get stream version

        Returns:
            int: version

        """
        return self._version

    @version.setter
    def version(self, value:int):
        """
        set stream version

        Args:
            value (int): version
        """
        self._version = int(value)

    @property
    def metadata_hash(self)->str:
        """
        get metadata hash

        Returns:
            str: metadata hash

        """
        return self._metadata_hash

    @metadata_hash.setter
    def metadata_hash(self, value: str):
        """
        set metadata hash

        Args:
            value (str): metadata hash
        """
        self._metadata_hash = value

    @property
    def data_descriptor(self)->DataDescriptor:
        """
        get stream data descriptor

        Returns:
            DataDescriptor: object of data descriptor
        """
        return self._dataDescriptor

    @data_descriptor.setter
    def data_descriptor(self, value: DataDescriptor):
        """
        Set stream data descriptor

        Args:
            value (DataDescriptor): object of data descriptor
        """
        self._dataDescriptor= value

    @property
    def modulez(self)->str:
        """
        get stream module metadata

        Returns:
            ModuleMetadata: object of ModuleMetadata
        """
        return self._module

    @modulez.setter
    def modulez(self, value:ModuleMetadata):
        """
        set stream module metadata

        Args:
            value (ModuleMetadata):  object of ModuleMetadata
        """
        self._module = value

    def set_name(self, value:str):
        """
        set name of a stream

        Args:
            value (str): name of a stream

        Returns:
            self

        """
        self._name = value
        return self

    def set_version(self, value:int):
        """
        set version of a stream

        Args:
            value (int): version of a stream

        Returns:
            self

        """
        self._version = value
        return self

    def add_description(self, stream_description:str):
        """
        Add stream description

        Args:
            stream_description (str): textual description of a stream

        Returns:
            self

        """
        self._description = stream_description
        return self

    def add_dataDescriptor(self, dd: DataDescriptor):
        """
        Add data description of a stream

        Args:
            dd (DataDescriptor): data descriptor

        Returns:
            self

        """
        self._dataDescriptor.append(dd)
        return self

    def add_module(self, mod: ModuleMetadata):
        """
        Add module metadata

        Args:
            mod (ModuleMetadata): module metadata

        Returns:
            self
        """
        self._module.append(mod)
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
        if not self._description:
            raise ValueError("Stream description is not defined.")
        for dd_obj in self.data_descriptor:
            if (dd_obj._name is None or dd_obj._name=="") and (dd_obj._type is None or dd_obj._type==""):
                raise ValueError("Name and/or type fields are missing in data descriptor.")
        for mm_obj in self.modulez:
            if (mm_obj._name is None or mm_obj._name=="") and (mm_obj._version is None or mm_obj._version==""):
                raise ValueError("Module name and/or version fields are missing in module info.")
            if len(mm_obj._authors)==0:
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
        for mm_obj in self.modulez:
            module_metadata.append(mm_obj.__dict__)
        metadata_json["name"] = self.name
        metadata_json["description"] = self._description
        metadata_json["data_descriptor"] = data_descriptor
        metadata_json["module"] = module_metadata
        return metadata_json

    def get_hash(self)->str:
        """
        Get the unique hash of metadata. Hash is generated based on "stream-name + data_descriptor + module-metadata"

        Returns:
            str: hash id of metadata
        """
        name = self.name
        version = self.version
        data_descriptor = ""
        modulez = ""
        for dd in self.data_descriptor:
            data_descriptor += str(dd._name+dd._type)
        for mm in self.modulez:
            modulez += str(mm._name) + str(mm._version) + str(mm._authors)
        hash_string = str(name)+str(data_descriptor)+str(modulez)
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
            module_info = metadata["module"]
            for dd in data_descriptors:
                data_descriptor_list.append(DataDescriptor().from_json(dd))

            for mm in module_info:
                module_list.append(ModuleMetadata().from_json(mm))

            md.data_descriptor = data_descriptor_list
            md.modulez = module_list
            md.name = metadata_json["name"]
            md._description = metadata.get("description", "")
            md.version = int(metadata_json["version"])
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

        md = Metadata()
        if isinstance(metadata, dict):
            data_descriptors = metadata["data_descriptor"]
            module_info = metadata["module"]
            for dd in data_descriptors:
                data_descriptor_list.append(DataDescriptor().from_json(dd))

            for mm in module_info:
                module_list.append(ModuleMetadata().from_json(mm))

            md.data_descriptor = data_descriptor_list
            md.modulez = module_list
            md.name = metadata.get("name", "")
            md._description = metadata.get("description", "")
            md.version = int(metadata.get("version", 0))
            md.metadata_hash = metadata.get("metadata_hash", "no-hash")
        return md
