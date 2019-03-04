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


class DataDescriptor():
    def __init__(self):
        """
        Constructor
        """
        self.name = None
        self.type = None
        self.attributes = {}

    def set_attribute(self, key, value):
        """
        Attributes field is option in metadata object. Arbitrary number or attributes could be attached to a DataDescriptor

        Args:
            key (str): key of an attribute
            value (str): value of an attribute

        Returns:
            self:
        Raises:
            ValueError: if key/value are missing

        """
        if key is None or key=="" or value is None or value=="":
            raise ValueError("Key and/or value cannot be None or empty.")
        self.attributes[key] = value
        return self

    def set_name(self, value):
        """
        Name of data descriptor

        Args:
            value (str): name

        Returns:
            self:
        """
        self.name = value
        return self

    def set_type(self, value:str):
        """
        Type of a data descriptor

        Args:
            value (str): type
        Returns:
            self:
        """
        self.type = value
        return self

    def from_json(self, obj):
        """
        Cast DataDescriptor class object into json

        Args:
            obj (DataDescriptor): object of a data descriptor class

        Returns:
            self:

        """
        self.__dict__ = obj
        return self

