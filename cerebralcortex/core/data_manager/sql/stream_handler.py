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

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


class StreamHandler:

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_stream_metadata(self, stream_name: str, version:str= "all") -> List[Metadata]:
        """
        Get a list of metadata for all versions available for a stream.

        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")

        Returns:
            list (Metadata): Returns an empty list if no metadata is available for a stream_name or a list of metadata otherwise.
        Raises:
            ValueError: stream_name cannot be None or empty.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_all_users("mperf")
            >>> [Metadata] # list of MetaData class objects
        """
        if stream_name is None or stream_name=="":
            raise ValueError("stream_name cannot be None or empty.")

        result = []
        if version=="all":
            qry = "SELECT * from " + self.datastreamTable +  ' where name=%(name)s'
            vals = {'name': str(stream_name)}
        else:
            qry = "SELECT * from " + self.datastreamTable +  ' where name=%s and version=%s'
            vals = stream_name,version

        rows = self.execute(qry, vals)
        if rows is not None and bool(rows):
            for row in rows:
                result.append(Metadata().from_json_sql(row))
            return result
        else:
            return []

    def list_streams(self)->List[Metadata]:
        """
        Get all the available stream names with metadata

        Returns:
            List[Metadata]: list of available streams metadata

        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.list_streams()
        """
        qry = "SELECT name from " + self.datastreamTable + " group by name, version"

        rows = self.execute(qry)
        results = []
        if rows:
            for row in rows:
                results.extend(self.get_stream_metadata(stream_name=row["name"]))
            return results
        else:
            return []

    def search_stream(self, stream_name):
        """
        Find all the stream names similar to stream_name arg. For example, passing "location"
        argument will return all stream names that contain the word location

        Returns:
            List[str]: list of stream names similar to stream_name arg

        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.search_stream("battery")
            >>> ["BATTERY--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST", "BATTERY--org.md2k.phonesensor--PHONE".....]
        """

        qry = "SELECT name from " + self.datastreamTable + " where name like %(name)s group by name, version"
        vals = {"name": "%"+str(stream_name).lower()+"%"}
        rows = self.execute(qry, vals)
        results = []
        if rows:
            for row in rows:
                results.append(row["name"])
            return results
        else:
            return []

    def get_stream_versions(self, stream_name: str) -> list:
        """
        Returns a list of versions available for a stream

        Args:
            stream_name (str): name of a stream
        Returns:
            list: list of int
        Raises:
            ValueError: if stream_name is empty or None
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_stream_versions("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> [1, 2, 4]
        """
        if not stream_name:
            raise ValueError("Stream_name is a required field.")

        versions = []

        qry = "select version from " + self.datastreamTable + " where name = %(name)s order by version ASC"
        vals = {"name":str(stream_name)}

        rows = self.execute(qry, vals)

        if len(rows) > 0:
            for row in rows:
                version = int(row["version"])
                versions.append(version)
            return versions
        else:
            []

    def get_stream_metadata_hash(self, stream_name: str) -> List[str]:
        """
        Get all the metadata_hash associated with a stream name.

        Args:
            stream_name (str): name of a stream
        Returns:
            list[str]: list of all the metadata hashes
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_metadata_hash("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> ["00ab666c-afb8-476e-9872-6472b4e66b68", "15cc444c-dfb8-676e-3872-8472b4e66b12"]
        """
        if not stream_name:
            raise ValueError("stream_name are required field.")
        metadata_hashes = []
        qry = "select metadata_hash from " + self.datastreamTable + " where name = %(name)s"
        vals = {"name":str(stream_name)}

        rows = self.execute(qry, vals)

        if len(rows) > 0:
            for row in rows:
                metadata_hashes.append(row["metadata_hash"])
            return metadata_hashes
        else:
            return []

    def get_stream_name(self, metadata_hash: uuid) -> str:
        """
       metadata_hash are unique to each stream version. This reverse look can return the stream name of a metadata_hash.

       Args:
           metadata_hash (uuid): This could be an actual uuid object or a string form of uuid.
       Returns:
           str: name of a stream
       Examples:
           >>> CC = CerebralCortex("/directory/path/of/configs/")
           >>> CC.get_stream_name("00ab666c-afb8-476e-9872-6472b4e66b68")
           >>> ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST
       """

        if not metadata_hash:
            raise ValueError("metadata_hash is a required field.")
        metadata_hash = str(metadata_hash)

        qry = "select name from " + self.datastreamTable + " where metadata_hash = %(metadata_hash)s"
        vals = {'metadata_hash': metadata_hash}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["name"]

    def get_stream_info_by_hash(self, metadata_hash: uuid) -> str:
        """
       metadata_hash are unique to each stream version. This reverse look can return the stream name of a metadata_hash.

       Args:
           metadata_hash (uuid): This could be an actual uuid object or a string form of uuid.
       Returns:
           dict: stream metadata and other info related to a stream
       Examples:
           >>> CC = CerebralCortex("/directory/path/of/configs/")
           >>> CC.get_stream_name("00ab666c-afb8-476e-9872-6472b4e66b68")
           >>> {"name": .....} # stream metadata and other information
       """

        if not metadata_hash:
            raise ValueError("metadata_hash is a required field.")
        metadata_hash = str(metadata_hash)

        qry = "select * from " + self.datastreamTable + " where metadata_hash = %(metadata_hash)s"
        vals = {'metadata_hash': metadata_hash}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return {}
        else:
            return rows[0]

    def is_stream(self, stream_name: str) -> bool:
        """
        Returns true if provided stream exists.

        Args:
            stream_name (str): name of a stream
        Returns:
            bool: True if stream_name exist False otherwise
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.is_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> True
        """
        qry = "SELECT * from " + self.datastreamTable + " where name = %(name)s"
        vals = {'name': str(stream_name)}
        rows = self.execute(qry, vals)

        if rows:
            return True
        else:
            return False


    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def save_stream_metadata(self, metadata_obj)->dict:
        """
        Update a record if stream already exists or insert a new record otherwise.

        Args:
            metadata_obj (Metadata): stream metadata
        Returns:
            dict: {"status": True/False,"verion":version}
        Raises:
             Exception: if fail to insert/update record in MySQL. Exceptions are logged in a log file
        """
        isQueryReady = 0

        metadata_hash = metadata_obj.get_hash()
        stream_name = metadata_obj.name

        is_metadata_changed = self._is_metadata_changed(stream_name, metadata_hash)
        status = is_metadata_changed.get("status")
        version = is_metadata_changed.get("version")

        metadata_obj.set_version(version)

        metadata_str = metadata_obj.to_json()
        if (status=="exist"):
            return {"status": True,"version":version, "record_type":"exist"}

        if (status == "new"):
            qry = "INSERT INTO " + self.datastreamTable + " (name, version, metadata_hash, metadata) VALUES(%s, %s, %s, %s)"
            vals = str(stream_name), str(version), str(metadata_hash), json.dumps(metadata_str)
            isQueryReady = 1

            # if nothing is changed then isQueryReady would be 0 and no database transaction would be performed
        if isQueryReady == 1:
            try:
                self.execute(qry, vals, commit=True)
                return {"status": True,"version":version, "record_type":"new"}
            except Exception as e:
                raise Exception(e)

    def _is_metadata_changed(self, stream_name, metadata_hash) -> dict:
        """
        Checks whether metadata_hash already exist in the system .

        Args:
            stream_name (str): name of a stream
            metadata_hash (str): hashed form of stream metadata
        Raises:
             Exception: if MySQL query fails

        Returns:
            dict: {"version": "version_number", "status":"new" OR "exist"}

        """
        version = 1
        qry = "select version from " + self.datastreamTable + " where metadata_hash = %(metadata_hash)s"
        vals = {"metadata_hash":metadata_hash}
        result = self.execute(qry, vals)

        if result:
            return {"version": version, "status":"exist"}
        else:
            stream_versions = self.get_stream_versions(stream_name)
            if bool(stream_versions):
                version = max(stream_versions)+1
                return {"version": version, "status":"new"}
            else:
                return {"version": 1, "status":"new"}
