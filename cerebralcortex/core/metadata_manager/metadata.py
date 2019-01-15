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

from cerebralcortex.core.metadata_manager.data_descriptor import DataDescriptor
from cerebralcortex.core.metadata_manager.module_info import ModuleMetadata
import json
import uuid

class Metadata():
    def __init__(self):
        """
        Metadata of a stream
        """
        self._name = None
        self._version = None
        self._metadata_hash = None
        self._dataDescriptor = []
        self._module = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, value):
        self._version = int(value)

    @property
    def metadata_hash(self):
        return self._metadata_hash

    @metadata_hash.setter
    def metadata_hash(self, value):
        self._metadata_hash = value

    @property
    def data_descriptor(self):
        return self._dataDescriptor

    @data_descriptor.setter
    def data_descriptor(self, value):
        self._dataDescriptor = value

    @property
    def modulez(self):
        return self._module

    @modulez.setter
    def modulez(self, value):
        self._module = value

    def add_dataDescriptor(self, dd: DataDescriptor):
        self._dataDescriptor.append(dd)
        return self

    def add_module(self, algo: ModuleMetadata):
        self._module.append(algo)
        return self

    def is_valid(self):
        for dd_obj in self.data_descriptor:
            if (dd_obj._name is None or dd_obj._name=="") or (dd_obj._type is None or dd_obj._type==""):
                raise Exception("Name and/or type fields are missing in data descriptor.")
        for mm_obj in self.modulez:
            if (mm_obj._module_name is None or mm_obj._module_name=="") or (mm_obj._version is None or mm_obj._version==""):
                raise Exception("Module name and/or version fields are missing in module info.")
            if len(mm_obj._authors)==0:
                raise Exception("Author information is missing.")
        return True

    @classmethod
    def to_json(cls):
        data_descriptor = []
        module_metadata = []
        metadata_json = {}
        for dd_obj in cls.data_descriptor:
            data_descriptor.append(dd_obj.__dict__)
        for mm_obj in cls.modulez:
            module_metadata.append(mm_obj.__dict__)
        metadata_json["name"] = cls.name
        metadata_json["data_descriptor"] = data_descriptor
        metadata_json["module"] = module_metadata
        return metadata_json

    @classmethod
    def from_json(cls, json_list):
        data_descriptor_list = []
        module_list = []
        metadata_list = []
        for tmp in json_list:
            metadata = json.loads(tmp.get("metadata"))
            data_descriptors = metadata["data_descriptor"]
            module_info = metadata["module"]
            for dd in data_descriptors:
                data_descriptor_list.append(DataDescriptor().from_json(dd))

            for mm in module_info:
                module_list.append(ModuleMetadata().from_json(mm))

            cls.data_descriptor = data_descriptor_list
            cls.modulez = module_list
            cls.name = tmp["name"]
            cls.version = int(tmp["version"])
            cls.metadata_hash = tmp["metadata_hash"]
            metadata_list.append(cls)

        return metadata_list

    @classmethod
    def get_hash(self):
        name = self.name
        version = self.version
        data_descriptor = ""
        modulez = ""
        for dd in self.data_descriptor:
            data_descriptor += str(dd.name+dd.type)
        for mm in self.modulez:
            modulez += str(mm.module_name)+str(mm.version)+str(mm.author)
        hash_string = str(name)+str(version)+str(data_descriptor)+str(modulez)
        hash_string = hash_string.strip().lower().replace(" ", "")

        return str(uuid.uuid3(uuid.NAMESPACE_DNS, hash_string))


