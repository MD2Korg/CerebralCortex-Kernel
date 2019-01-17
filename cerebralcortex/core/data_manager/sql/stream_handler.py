# Copyright (c) 2018, MD2K Center of Excellence
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
import traceback
import uuid
from typing import List
from cerebralcortex.core.metadata_manager.metadata import Metadata


class StreamHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    # def get_stream_metadata_by_hash(self, metadata_hash: uuid) -> dict:
    #     """
    #     Get stream metadata
    #     :param metadata_hash:
    #     :return: row_id, user_id, name, metadata_hash, metadata
    #     :rtype dict
    #     """
    #     qry = "SELECT * from " + self.datastreamTable + " where metadata_hash=%(metadata_hash)s"
    #     vals = {"metadata_hash": str(metadata_hash)}
    #     rows = self.execute(qry, vals)
    #     return rows

    def get_stream_metadata(self, stream_name: str, version:str= "all") -> list(Metadata):
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
                result.append(row)
            result = Metadata.from_json(result)
            return result
        else:
            return []

    # def get_stream_metadata_by_user(self, user_id: uuid, stream_name: str = None) -> dict:
    #     """
    #     Returns stream ids and metadata of a stream name belong to a user
    #     :param user_id:
    #     :return: row_id, user_id, name, metadata_hash, metadata
    #     :rtype: dict
    #     """
    #     vals = []
    #     if not user_id:
    #         raise ValueError("User ID cannot be empty/None.")
    #
    #     qry = "SELECT * from " + self.datastreamTable
    #     where_clause = " where user_id=%s "
    #     vals.append(user_id)
    #     if stream_name:
    #         where_clause += " and name=%s "
    #         vals.append(stream_name)
    #
    #     qry = qry + where_clause
    #     vals = tuple(vals)
    #     rows = self.execute(qry, vals)
    #     return rows

    # def user_has_stream(self, user_id: uuid, stream_name: str) -> bool:
    #     """
    #     Returns true if a user has a stream available
    #     :param user_id:
    #     :param stream_name:
    #     :return: True if owner has a stream, False otherwise
    #     """
    #     if not stream_name or not user_id:
    #         raise ValueError("Strea name and User ID are required fields.")
    #
    #     qry = "select stream_id from " + self.datastreamTable + " where user_id=%s and name = %s"
    #     vals = str(user_id), str(stream_name)
    #
    #     rows = self.execute(qry, vals)
    #
    #     if len(rows) > 0:
    #         return True
    #     else:
    #         return False

    def get_stream_versions(self, stream_name: str) -> bool:
        """
        Returns a list of all available version of a stream
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

    def get_all_users(self, study_name: str) -> list[dict]:
        """
        Get a list of all users part of a study.
        Args:
            study_name (str): name of a study
        Raises:
            ValueError: Study name is a requied field.
        Returns:
            list(dict): Returns empty list if there is no user associated to the study_name and/or study_name does not exist.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_all_users("mperf")
            >>> [{"76cc444c-4fb8-776e-2872-9472b4e66b16": "nasir_ali"}] # [{user_id, user_name}]
        """
        if not study_name:
            raise ValueError("Study name is a requied field.")

        results = []
        qry = 'SELECT user_id, username FROM ' + self.userTable + ' where user_metadata->"$.study_name"=%(study_name)s'
        vals = {'study_name': str(study_name)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return []
        else:
            for row in rows:
                results.append(row)
            return results

    # def get_user_streams(self, user_id: uuid) -> dict:
    #
    #     """
    #     Returns all user streams with name and metadata attached to it. Do not use "id" field as it doesn't
    #     represents all the stream-ids linked to a stream name. Use stream_ids field in dict to get all stream-ids of a stream name.
    #     :param user_id:
    #     :return: id, user_id, name, data_descriptor,execution_context,annotations, type, start_time, end_time, tmp, stream_ids (this contains all the stream ids of a stream name)
    #     :rtype: dict
    #     """
    #     if not user_id:
    #         raise ValueError("User ID is a required field.")
    #
    #     result = {}
    #     qry = 'SELECT * FROM ' + self.datastreamTable + ' where user_id=%(user_id)s'
    #     vals = {'user_id': str(user_id)}
    #
    #     rows = self.execute(qry, vals)
    #
    #     if len(rows) == 0:
    #         return result
    #     else:
    #         for row in rows:
    #             stream_ids = self.get_stream_metadata_hash(str(user_id), row["name"])
    #             row["stream_ids"] = [d['id'] for d in stream_ids]
    #             result[row["name"]] = row
    #         return result

    # def get_user_streams_metadata(self, user_id: str) -> dict:
    #     """
    #     Get all streams metadata of a user
    #     """
    #     if not user_id:
    #         raise ValueError("User ID is a required field.")
    #
    #     result = {}
    #     qry = "select name, metadata from " + self.datastreamTable + " where user_id = %(user_id)s"
    #     vals = {'user_id': str(user_id)}
    #
    #     rows = self.execute(qry, vals)
    #
    #     if len(rows) == 0:
    #         return result
    #     else:
    #         for row in rows:
    #             stream_ids = self.get_stream_metadata_hash(str(user_id), row["name"])
    #             row["stream_ids"] = [d['id'] for d in stream_ids]
    #             result[row["name"]] = row
    #         return result

    def get_user_name(self, user_id: str) -> str:
        """
        Get the user name linked to a user id.

        Args:
            user_name (str): username of a user
        Returns:
            bool: user_id associated to username
        Raises:
            ValueError: User ID is a required field.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_user_name("76cc444c-4fb8-776e-2872-9472b4e66b16")
            >>> 'nasir_ali'
        """
        if not user_id:
            raise ValueError("User ID is a required field.")

        qry = "select username from " + self.userTable + " where user_id = %(user_id)s"
        vals = {'user_id': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["username"]

    def is_user(self, user_id: uuid = None, user_name: uuid = None) -> bool:
        """
        Checks whether a user exists in the system. One of both parameters could be set to verify whether user exist.

        Args:
            user_id (str): id (uuid) of a user
            user_name (str): username of a user
        Returns:
            bool: True if a user exists in the system or False otherwise.
        Raises:
            ValueError: Both user_id and user_name cannot be None or empty.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.is_user(user_id="76cc444c-4fb8-776e-2872-9472b4e66b16")
            >>> True
        """
        if user_id and user_name:
            qry = "select username from " + self.userTable + " where user_id = %s and username=%s"
            vals = str(user_id), user_name
        elif user_id and not user_name:
            qry = "select username from " + self.userTable + " where user_id = %(user_id)s"
            vals = {'user_id': str(user_id)}
        elif not user_id and user_name:
            qry = "select username from " + self.userTable + " where username = %(username)s"
            vals = {'username': str(user_name)}
        else:
            raise ValueError("Both user_id and user_name cannot be None or empty.")

        rows = self.execute(qry, vals)

        if len(rows) > 0:
            return True
        else:
            return False

    def get_user_id(self, user_name: str) -> str:
        """
        Get the user id linked to user_name.

        Args:
            user_name (str): username of a user
        Returns:
            str: user id associated to user_name
        Raises:
            ValueError: User name is a required field.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_user_id("nasir_ali")
            >>> '76cc444c-4fb8-776e-2872-9472b4e66b16'
        """
        if not user_name:
            raise ValueError("User name is a required field.")

        qry = "select user_id from " + self.userTable + " where username = %(username)s"
        vals = {'username': str(user_name)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["user_id"]

    def get_stream_metadata_hash(self, stream_name: str) -> list:
        """
        Get all the metadata_hash associated with a stream name.
        Args:
            stream_name (str): name of a stream
        Returns:
            list(str): list of all the metadata hashes
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

    def save_stream_metadata(self, metadata_obj):
        """
        Update a record if stream already exists, insert a new record otherwise.
        """
        isQueryReady = 0

        metadata_hash = metadata_obj.get_hash()
        stream_name = metadata_obj.name

        is_metadata_changed = self.is_metadata_changed(stream_name, metadata_hash)
        status = is_metadata_changed.get("status")
        version = is_metadata_changed.get("version")

        metadata_str = metadata_obj.to_json()

        if (status == "new"):
            qry = "INSERT INTO " + self.datastreamTable + " (name, version, metadata_hash, metadata) VALUES(%s, %s, %s, %s)"
            vals = str(stream_name), str(version), str(metadata_hash), json.dumps(metadata_str)
            isQueryReady = 1

            # if nothing is changed then isQueryReady would be 0 and no database transaction would be performed
        if isQueryReady == 1:
            try:
                self.execute(qry, vals, commit=True)
            except:
                self.logging.log(
                    error_message="Query: " + str(qry) + " - cannot be processed. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)

    def is_metadata_changed(self, stream_name, metadata_hash) -> str:
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



    ###########################################################################################################################
    ##                                          DATA REPLAY HELPER METHOD
    ###########################################################################################################################

    def mark_processed_day(self, user_id: uuid, stream_id: uuid, day: str):
        """
        Mark row as processed if the data has been processed/ingested. This is used for data replay.
        :param user_id:
        :param stream_id:
        :param day:
        """
        qry = "UPDATE " + self.dataReplayTable + " set processed=1 where owner_id=%s and stream_id=%s and day=%s"
        vals = str(user_id), str(stream_id), str(day)
        self.execute(qry, vals, commit=True)

    def is_day_processed(self, user_id: uuid, stream_id: uuid, day: str) -> bool:
        """
        Checks whether data is processed for a given user-id and stream-id
        :param user_id:
        :param stream_id:
        :param day:
        :return: True if day is processed, False otherwise
        :rtype: bool
        """
        if day is not None:
            qry = "SELECT processed from " + self.dataReplayTable + " where owner_id=%s and stream_id=%s and day=%s"
            vals = str(user_id), str(stream_id), str(day)
            rows = self.execute(qry, vals)
            if rows[0]["processed"] == 1:
                return True
            else:
                return False
        return False


    def get_all_data_days(self):
        """
        Returns a list of days where data is available
        :return: List of days (yyyymmdd)
        :rtype: list of strings
        """
        qry = "SELECT day from " + self.dataReplayTable + " where processed=0 group by day"
        rows = self.execute(qry)
        days = []
        if len(rows) > 0:
            for row in rows:
                days.append(row["day"])
        return days

    def get_replay_batch(self, day, record_limit: int = 5000, nosql_blacklist:dict={"regzex":"nonez", "txt_match":"nonez"}) -> List:
        """
        This method helps in data replay. Yield a batch of data rows that needs to be processed and ingested in CerebralCortex
        :param record_limit:
        :return: List of dicts (keys=owner_id, stream_id, stream_name, day, files_list, metadata)
        :rtype: dict
        """

        regex_cols = ""  # regex match on columns
        like_cols = ""  # like operator on columns
        for breg in nosql_blacklist["regzex"]:
            regex_cols += '%s NOT REGEXP "%s" and ' % ("stream_name", nosql_blacklist["regzex"][breg])

        for btm in nosql_blacklist["txt_match"]:
            like_cols += '%s not like "%s" and ' % ("stream_name", nosql_blacklist["txt_match"][btm])

        # if regex_cols!="" or like_cols!="":
        #     where_clause = " where "

        if regex_cols != "" and like_cols != "":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from " + self.dataReplayTable + " where " + regex_cols + " " + like_cols + "  processed=0 and day='"+day+"' order by dir_size"
        elif regex_cols != "" and like_cols == "":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from " + self.dataReplayTable + " where " + regex_cols +"  processed=0 and day='"+day+"' order by dir_size"
        elif regex_cols == "" and like_cols != "":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from " + self.dataReplayTable + " where " + like_cols + "  processed=0 and day='"+day+"' order by dir_size"
        else:
            qry = ""

        if qry != "":
            rows = self.execute(qry)
            msgs = []
            if len(rows) > 0:
                for row in rows:
                    if len(msgs) > int(record_limit):
                        yield msgs
                        msgs = []
                    #if row["owner_id"] in good_participants:
                    msgs.append(
                        {"owner_id": row["owner_id"], "stream_id": row["stream_id"], "stream_name": row["stream_name"],
                         "metadata": json.loads(row["metadata"]), "day": row["day"],
                         "filename": json.loads(row["files_list"])})
                yield msgs
            else:
                yield []
        else:
            yield []

    def add_to_data_replay_table(self, table_name, owner_id, stream_id, stream_name, day, files_list, dir_size, metadata):
        qry = "INSERT IGNORE INTO "+table_name+" (owner_id, stream_id, stream_name, day, files_list, dir_size, metadata) VALUES(%s, %s, %s, %s, %s, %s, %s)"
        vals = str(owner_id), str(stream_id), str(stream_name), str(day), json.dumps(files_list), dir_size, json.dumps(metadata)
        self.execute(qry, vals, commit=True)
