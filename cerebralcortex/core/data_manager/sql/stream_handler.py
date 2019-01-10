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


class StreamHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_stream_metadata_by_hash(self, metadata_hash: uuid) -> dict:
        """
        Get stream metadata
        :param metadata_hash:
        :return: row_id, user_id, name, metadata_hash, metadata
        :rtype dict
        """
        qry = "SELECT * from " + self.datastreamTable + " where metadata_hash=%(metadata_hash)s"
        vals = {"metadata_hash": str(metadata_hash)}
        rows = self.execute(qry, vals)
        return rows

    def get_stream_metadata_by_name(self, stream_name: str) -> dict:
        """
        Get stream metadata
        :param stream_id:
        :return: row_id, user_id, name, metadata_hash, metadata
        :rtype dict
        """
        qry = "SELECT * from " + self.datastreamTable +  ' where name=%(name)s'
        vals = {'name': str(stream_name)}
        rows = self.execute(qry, vals)
        if rows is not None and bool(rows):
            return rows
        else:
            return []

    def get_stream_metadata_by_user(self, user_id: uuid, stream_name: str = None) -> dict:
        """
        Returns stream ids and metadata of a stream name belong to a user
        :param user_id:
        :return: row_id, user_id, name, metadata_hash, metadata
        :rtype: dict
        """
        vals = []
        if not user_id:
            raise ValueError("User ID cannot be empty/None.")

        qry = "SELECT * from " + self.datastreamTable
        where_clause = " where user_id=%s "
        vals.append(user_id)
        if stream_name:
            where_clause += " and name=%s "
            vals.append(stream_name)

        qry = qry + where_clause
        vals = tuple(vals)
        rows = self.execute(qry, vals)
        return rows

    def user_has_stream(self, user_id: uuid, stream_name: str) -> bool:
        """
        Returns true if a user has a stream available
        :param user_id: 
        :param stream_name: 
        :return: True if owner has a stream, False otherwise
        """
        if not stream_name or not user_id:
            raise ValueError("Strea name and User ID are required fields.")

        qry = "select stream_id from " + self.datastreamTable + " where user_id=%s and name = %s"
        vals = str(user_id), str(stream_name)

        rows = self.execute(qry, vals)

        if len(rows) > 0:
            return True
        else:
            return False

    def stream_versions(self, user_id: uuid, stream_name: str) -> bool:
        """
        Returns a list of all available version of a stream
        """
        if not stream_name or not user_id:
            raise ValueError("Strea name and User ID are required fields.")

        versions = []

        qry = "select version from " + self.datastreamTable + " where user_id=%s and name = %s order by version ASC"
        vals = str(user_id), str(stream_name)

        rows = self.execute(qry, vals)

        if len(rows) > 0:
            for row in rows:
                version = int(row["version"])
                versions.append(version)
            return versions
        else:
            {}

    def get_all_users(self, study_name: str) -> List[dict]:

        """
        Get all users id and user name for a particular study
        :param study_name:
        :return: List of dicts (keys=id, username)
        :rtype List
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

    def get_user_streams(self, user_id: uuid) -> dict:

        """
        Returns all user streams with name and metadata attached to it. Do not use "id" field as it doesn't
        represents all the stream-ids linked to a stream name. Use stream_ids field in dict to get all stream-ids of a stream name.
        :param user_id:
        :return: id, user_id, name, data_descriptor,execution_context,annotations, type, start_time, end_time, tmp, stream_ids (this contains all the stream ids of a stream name)
        :rtype: dict
        """
        if not user_id:
            raise ValueError("User ID is a required field.")

        result = {}
        qry = 'SELECT * FROM ' + self.datastreamTable + ' where user_id=%(user_id)s'
        vals = {'user_id': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return result
        else:
            for row in rows:
                stream_ids = self.get_stream_metadata_hash(str(user_id), row["name"])
                row["stream_ids"] = [d['id'] for d in stream_ids]
                result[row["name"]] = row
            return result

    def get_user_streams_metadata(self, user_id: str) -> dict:
        """
        Get all streams metadata of a user
        """
        if not user_id:
            raise ValueError("User ID is a required field.")

        result = {}
        qry = "select name, metadata from " + self.datastreamTable + " where user_id = %(user_id)s"
        vals = {'user_id': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return result
        else:
            for row in rows:
                stream_ids = self.get_stream_metadata_hash(str(user_id), row["name"])
                row["stream_ids"] = [d['id'] for d in stream_ids]
                result[row["name"]] = row
            return result

    def get_user_name(self, user_id: uuid) -> str:
        """
        Get username of a user's UUID
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
        Check whether a username or user ID exists in MySQL
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
            raise ValueError("Wrong parameters.")

        rows = self.execute(qry, vals)

        if len(rows) > 0:
            return True
        else:
            return False

    def get_user_id(self, user_name: str) -> str:
        """
        Get user's UUID
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

    def get_stream_metadata_hash(self, user_id: uuid, stream_name: str) -> dict:
        """
        Get a stream ids of stream name linked to a user
        """
        if not stream_name or not user_id:
            raise ValueError("User ID and stream name are required field.")

        qry = "select metadata_hash from " + self.datastreamTable + " where user_id=%s and name = %s"
        vals = str(user_id), str(stream_name)

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return {}
        else:
            return rows

    def get_stream_name(self, metadata_hash: uuid) -> str:
        """
        Get strea name linked to a stream UUID
        """
        if not metadata_hash:
            raise ValueError("metadata_hash is a required field.")

        qry = "select name from " + self.datastreamTable + " where metadata_hash = %(metadata_hash)s"
        vals = {'metadata_hash': str(metadata_hash)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["name"]

    def is_stream(self, stream_name: uuid) -> bool:

        """
        Checks whether a stream exist
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

    def save_stream_metadata(self, stream_name: str, user_id: uuid, metadata: dict, stream_type: str):
        """
        Update a record if stream already exists, insert a new record otherwise.
        """
        isQueryReady = 0

        metadata_hash = self.metadata_to_hash(stream_name, user_id, metadata)
        is_metadata_changed = self.is_metadata_changed(metadata_hash)
        status = is_metadata_changed.get("status")
        version = is_metadata_changed.get("version")

        if status == "changed":
            # update annotations and end-time
            qry = "UPDATE " + self.datastreamTable + " set metadata=metadata, version=%s where metadata_hash=%s"
            vals = json.dumps(metadata, default=str), str(version), str(metadata_hash)
            isQueryReady = 1
        elif (is_metadata_changed == "new"):
            qry = "INSERT INTO " + self.datastreamTable + " (user_id, name, version, metadata_hash, metadata) VALUES(%s, %s, %s, %s, %s)"
            vals = str(user_id), str(stream_name), str(version), str(metadata_hash), json.dumps(metadata)
            isQueryReady = 1

            # if nothing is changed then isQueryReady would be 0 and no database transaction would be performed
        if isQueryReady == 1:
            try:
                self.execute(qry, vals, commit=True)
            except:
                self.logging.log(
                    error_message="Query: " + str(qry) + " - cannot be processed. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)

    def is_metadata_changed(self, stream_name, user_id, metadata_hash) -> str:
        version = 1
        qry = "select version from " + self.datastreamTable + " where metadata_hash = %(metadata_hash)s"
        vals = {"metadata_hash":metadata_hash}
        result = self.execute(qry, vals)

        if result:
            return {"version": version, "status":"unchanged"}
        else:
            stream_versions = self.stream_versions(user_id, stream_name)
            if bool(stream_versions):
                version = max(stream_versions)
                return {"version": version, "status":"changed"}
            else:
                return {"version": version, "status":"new"}


    def metadata_to_hash(self, stream_name, user_id, metadata):
        text = str(stream_name)+str(user_id)+str(metadata)
        metadata_hash = uuid.uuid3(uuid.NAMESPACE_DNS, text)
        return metadata_hash


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
