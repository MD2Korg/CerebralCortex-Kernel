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


from cerebralcortex.core.datatypes import DataStream


class BlueprintStorage():
    """
    This is a sample reference class. If you want to add another storage layer then the class must have following methods in it.
    read_file()
    write_file()
    """
    def __init__(self, obj):
        self.obj = obj

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def read_file(self, stream_name:str, version:str="all")->object:
        """
        Get stream data from storage system. Data would be return as pyspark DataFrame object
        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")
        Returns:
            object: pyspark DataFrame object
        Raises:
            Exception: if stream name does not exist.
        """
        # TODO: implement your own storage layer to read data
        pass

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def write_file(self, stream_name:str, data:DataStream) -> bool:
        """
        Write pyspark DataFrame to a data storage system
        Args:
            stream_name (str): name of the stream
            data (object): pyspark DataFrame object

        Returns:
            bool: True if data is stored successfully or throws an Exception.
        Raises:
            Exception: if DataFrame write operation fails
        """

        # TODO: implement your own storage layer to write data
        pass
