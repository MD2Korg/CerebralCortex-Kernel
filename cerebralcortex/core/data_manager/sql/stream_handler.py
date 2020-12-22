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

import uuid
from typing import List

from cerebralcortex.core.data_manager.sql.orm_models import Stream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


class StreamHandler:

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def save_stream_metadata(self, metadata_obj) -> dict:
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

        if isinstance(metadata_obj, Metadata):
            metadata_hash = metadata_obj.get_hash()
            metadata_obj = metadata_obj.to_json()
        else:
            raise Exception("Metadata is not type of MetaData object class.")

        stream_name = metadata_obj.get("name")

        is_metadata_changed = self._is_metadata_changed(stream_name, metadata_hash)
        status = is_metadata_changed.get("status")
        version = is_metadata_changed.get("version")

        if (status == "exist"):
            return {"status": True, "version": version, "record_type": "exist"}

        if (status == "new"):
            stream  = Stream(name=stream_name.lower(), version=version, study_name=self.study_name, metadata_hash=str(metadata_hash), stream_metadata=metadata_obj)
            isQueryReady = 1

        # if nothing is changed then isQueryReady would be 0 and no database transaction would be performed
        if isQueryReady == 1:
            try:
                self.session.add(stream)
                self.session.commit()
                return {"status": True, "version": version, "record_type": "new"}
            except Exception as e:
                self.session.rollback()
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
        rows = self.session.query(Stream).filter(Stream.metadata_hash==metadata_hash).first()

        self.close()

        if rows:
            return {"version": int(rows.version), "status": "exist"}
        else:
            stream_versions = self.get_stream_versions(stream_name)
            if bool(stream_versions):
                version = max(stream_versions) + 1
                return {"version": version, "status": "new"}
            else:
                return {"version": 1, "status": "new"}

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_stream_metadata_by_name(self, stream_name: str, version:int) -> Metadata:
        """
        Get a list of metadata for all versions available for a stream.

        Args:
            stream_name (str): name of a stream
            version (int): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")

        Returns:
            Metadata: Returns an empty list if no metadata is available for a stream_name or a list of metadata otherwise.
        Raises:
            ValueError: stream_name cannot be None or empty.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.list_users("mperf")
            >>> [Metadata] # list of MetaData class objects
        """
        if stream_name is None or stream_name=="":
            raise ValueError("stream_name cannot be None or empty.")

        rows = self.session.query(Stream.stream_metadata).filter((Stream.name == stream_name) & (Stream.version==version) & (Stream.study_name==self.study_name)).first()

        self.close()

        if rows:
            return Metadata().from_json_file(rows.stream_metadata)
        else:
            return None

    def list_streams(self)->List[Metadata]:
        """
        Get all the available stream names with metadata

        Returns:
            List[Metadata]: list of available streams metadata [{name:"", metadata:""}...]

        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.list_streams()
        """
        rows = self.session.query(Stream.stream_metadata).filter(Stream.study_name == self.study_name).all()
        self.close()
        results = []
        if rows:
            for row in rows:
                results.append(Metadata().from_json_file(row.stream_metadata))
            return results
        else:
            return results

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
        rows = self.session.query(Stream.name).filter(Stream.name.ilike('%'+stream_name+'%')).all()
        self.close()
        if rows:
            return rows
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

        rows = self.session.query(Stream.version).filter((Stream.name==stream_name) & (Stream.study_name==self.study_name)).all()
        self.close()
        results = []
        if rows:
            for row in rows:
                results.append(row.version)
            return results
        else:
            return results

    def get_stream_metadata_hash(self, stream_name: str) -> List:
        """
        Get all the metadata_hash associated with a stream name.

        Args:
            stream_name (str): name of a stream
        Returns:
            list: list of all the metadata hashes with name and versions
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_metadata_hash("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> [["stream_name", "version", "metadata_hash"]]
        """
        if not stream_name:
            raise ValueError("stream_name are required field.")

        rows = self.session.query(Stream.name, Stream.version, Stream.metadata_hash).filter((Stream.name == stream_name) & (Stream.study_name==self.study_name)).all()
        self.close()
        if rows:
            return rows
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

        rows = self.session.query(Stream.name).filter((Stream.metadata_hash == metadata_hash) & (Stream.study_name==self.study_name)).first()
        self.close()
        if rows:
            return rows.name
        else:
            raise Exception(str(metadata_hash)+ " does not exist.")

    def get_stream_metadata_by_hash(self, metadata_hash: uuid) -> List:
        """
       metadata_hash are unique to each stream version. This reverse look can return the stream name of a metadata_hash.

       Args:
           metadata_hash (uuid): This could be an actual uuid object or a string form of uuid.
       Returns:
           List: [stream_name, metadata]
       Examples:
           >>> CC = CerebralCortex("/directory/path/of/configs/")
           >>> CC.get_stream_name("00ab666c-afb8-476e-9872-6472b4e66b68")
           >>> ["name" .....] # stream metadata and other information
       """

        if not metadata_hash:
            raise ValueError("metadata_hash is a required field.")

        rows = self.session.query(Stream).filter((Stream.metadata_hash == metadata_hash) & (Stream.study_name == self.study_name)).first()
        self.close()
        if rows:
            stream_info = rows.stream_metadata
            stream_info["version"] = rows.version
            return stream_info
        else:
            raise Exception(str(metadata_hash)+ " does not exist.")

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

        rows = self.session.query(Stream.name).filter(
            (Stream.name == stream_name) & (Stream.study_name == self.study_name)).first()
        self.close()
        if rows:
            return True
        else:
            return False

    def _delete_stream(self, stream_name: str) -> bool:
        """
        Returns true if provided stream name is deleted.

        Args:
            stream_name (str): name of a stream
        Returns:
            bool: True if stream_name is deleted False otherwise
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC._delete_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> True
        """

        rows = self.session.query(Stream.name).filter(
            (Stream.name == stream_name) & (Stream.study_name == self.study_name))

        try:
            rows.delete(synchronize_session=False)
            self.session.commit()
            self.close()
            return True
        except Exception as e:
            raise e
            return False

