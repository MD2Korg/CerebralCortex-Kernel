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
import re
import traceback
import uuid
from datetime import datetime, timedelta
from typing import List

from pytz import timezone


class StreamHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_stream_metadata(self, stream_id: uuid) -> dict:
        """
        Get stream metadata
        :param stream_id:
        :return: identifier, owner, name, data_descriptor,execution_context,annotations, type, start_time, end_time, tmp (tmp is just a primary key ID)
        :rtype dict
        """
        qry = "SELECT * from " + self.datastreamTable + " where identifier=%(identifier)s"
        vals = {"identifier": str(stream_id)}
        rows = self.execute(qry, vals)
        return rows

    def get_stream_metadata_by_user(self, user_id: uuid, stream_name: str = None, start_time: datetime = None,
                                    end_time: datetime = None) -> dict:
        """
        Returns stream ids and metadata of a stream name belong to a user
        :param user_id:
        :return: identifier, data_descriptor,execution_context,annotations, start_time, end_time
        :rtype: dict
        """
        vals = []
        if not user_id:
            raise ValueError("User ID cannot be empty/None.")

        qry = "SELECT identifier, data_descriptor,execution_context,annotations, start_time, end_time from " + self.datastreamTable
        where_clause = " where owner=%s "
        vals.append(user_id)
        if stream_name:
            where_clause += " and name=%s "
            vals.append(stream_name)
        if start_time:
            where_clause += " and start_time<=%s "
            vals.append(start_time)
        if end_time:
            where_clause += " and end_time>=%s "
            vals.append(end_time)

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

        qry = "select identifier from " + self.datastreamTable + " where owner=%s and name = %s"
        vals = str(user_id), str(stream_name)

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return False
        else:
            return True

    def get_stream_duration(self, stream_id: uuid) -> dict:
        """
        Get time duration (start time - end time) of a stream
        :param stream_id:
        :return:start_time, end_time
        :rtype dict
        """
        if not stream_id:
            raise ValueError("Stream ID is a required field.")

        qry = "select start_time, end_time from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return {"start_time": None, "end_time": None}
        else:
            return {"start_time": rows[0]["start_time"], "end_time": rows[0]["end_time"]}

    def get_all_users(self, study_name: str) -> List[dict]:

        """
        Get all users id and user name for a particular study
        :param study_name:
        :return: List of dicts (keys=identifier, username)
        :rtype List
        """
        if not study_name:
            raise ValueError("Study name is a requied field.")

        results = []
        qry = 'SELECT identifier, username FROM ' + self.userTable + ' where user_metadata->"$.study_name"=%(study_name)s'
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
        Returns all user streams with name and metadata attached to it. Do not use "identifier" field as it doesn't
        represents all the stream-ids linked to a stream name. Use stream_ids field in dict to get all stream-ids of a stream name.
        :param user_id:
        :return: identifier, owner, name, data_descriptor,execution_context,annotations, type, start_time, end_time, tmp, stream_ids (this contains all the stream ids of a stream name)
        :rtype: dict
        """
        if not user_id:
            raise ValueError("User ID is a required field.")

        result = {}
        qry = 'SELECT * FROM ' + self.datastreamTable + ' where owner=%(owner)s'
        vals = {'owner': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return result
        else:
            for row in rows:
                stream_ids = self.get_stream_id(str(user_id), row["name"])
                row["stream_ids"] = [d['identifier'] for d in stream_ids]
                result[row["name"]] = row
            return result

    def get_user_streams_metadata(self, user_id: str) -> dict:
        """
        Get all streams metadata of a user
        :param user_id:
        :return: name, data_descriptor,execution_context,annotations, start_time, end_time
        :rtype: dict
        """
        if not user_id:
            raise ValueError("User ID is a required field.")

        result = {}
        qry = "select name, data_descriptor,execution_context,annotations, start_time, end_time from " + self.datastreamTable + " where owner = %(owner)s"
        vals = {'owner': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return result
        else:
            for row in rows:
                stream_ids = self.get_stream_id(str(user_id), row["name"])
                row["stream_ids"] = [d['identifier'] for d in stream_ids]
                result[row["name"]] = row
            return result

    def get_user_name(self, user_id: uuid) -> str:
        """
        Get username of a user's UUID
        :param user_id:
        :return: user id
        :rtype: str
        """
        if not user_id:
            raise ValueError("User ID is a required field.")

        qry = "select username from " + self.userTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["username"]

    def is_user(self, user_id: uuid = None, user_name: uuid = None) -> bool:
        """
        Check whether a username or user ID exists in MySQL
        :param user_id:
        :param user_name
        :return: True if user exist, False otherwise
        :rtype: bool
        """
        if user_id and user_name:
            qry = "select username from " + self.userTable + " where identifier = %s and username=%s"
            vals = str(user_id), user_name
        elif user_id and not user_name:
            qry = "select username from " + self.userTable + " where identifier = %(identifier)s"
            vals = {'identifier': str(user_id)}
        elif not user_id and user_name:
            qry = "select username from " + self.userTable + " where username = %(username)s"
            vals = {'username': str(user_name)}
        else:
            raise ValueError("Wrong parameters.")

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return False
        else:
            return True

    def get_user_id(self, user_name: str) -> str:
        """
        Get user's UUID
        :param user_name:
        :return: user's UUID
        :rtype: str
        """
        if not user_name:
            raise ValueError("User name is a required field.")

        qry = "select identifier from " + self.userTable + " where username = %(username)s"
        vals = {'username': str(user_name)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["identifier"]

    def get_stream_id(self, user_id: uuid, stream_name: str) -> dict:
        """
        Get a stream ids of stream name linked to a user
        :param user_id
        :param stream_name:
        :return: List of stream ids
        :rtype: dict
        """
        if not stream_name or not user_id:
            raise ValueError("User ID and stream name are required field.")

        qry = "select identifier from " + self.datastreamTable + " where owner=%s and name = %s"
        vals = str(user_id), str(stream_name)

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return {}
        else:
            return rows

    def get_stream_days(self, stream_id: uuid) -> List:
        """
        Returns a list of days (string format: YearMonthDay (e.g., 20171206) for a given stream-id
        :param stream_id:
        :param dd_stream_id:
        :return: list of days (format YYYYMMDD)
        :rtype: List
        """
        if not stream_id:
            raise ValueError("Stream ID is a required field.")
        all_days = []
        stream_days = self.get_stream_duration(stream_id)
        if stream_days["end_time"] is not None and stream_days["start_time"] is not None:
            days = stream_days["end_time"] - stream_days["start_time"]
            for day in range(days.days + 1):
                all_days.append((stream_days["start_time"] + timedelta(days=day)).strftime('%Y%m%d'))

        return all_days

    def get_stream_name(self, stream_id: uuid) -> str:
        """
        Get strea name linked to a stream UUID
        :param stream_id:
        :return: stream name
        :rtype: str
        """
        if not stream_id:
            raise ValueError("Stream ID is a required field.")

        qry = "select name from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["name"]

    def is_stream(self, stream_id: uuid) -> bool:

        """
        Checks whether a stream exist
        :param stream_id:
        :return: True if a stream ID exist, False otherwise
        :rtype: bool
        """
        qry = "SELECT * from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}
        rows = self.execute(qry, vals)

        if rows:
            return True
        else:
            return False

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def save_stream_metadata(self, stream_id: uuid, stream_name: str, owner_id: uuid,
                             data_descriptor: dict,
                             execution_context: dict,
                             annotations: dict, stream_type: str, start_time: datetime, end_time: datetime):
        """
        Update a record if stream already exists, insert a new record otherwise.
        :param stream_id:
        :param stream_name:
        :param owner_id:
        :param data_descriptor:
        :param execution_context:
        :param annotations:
        :param stream_type:
        :param start_time:
        :param end_time:
        """
        isQueryReady = 0

        if stream_id:
            stream_id = str(stream_id)

        if self.is_stream(stream_id=stream_id):
            self.update_start_time(stream_id, start_time)
            stream_end_time = self.check_end_time(stream_id, end_time)
            annotation_status = self.annotations_status(stream_id, owner_id, annotations)
        else:
            stream_end_time = None
            annotation_status = "new"

        if stream_end_time != "unchanged" and annotation_status == "changed":
            # update annotations and end-time
            qry = "UPDATE " + self.datastreamTable + " set annotations=JSON_ARRAY_APPEND(annotations, '$.annotations',  CAST(%s AS JSON)), end_time=%s where identifier=%s"
            vals = json.dumps(annotations, default=str), stream_end_time, str(stream_id)
            isQueryReady = 1
        elif stream_end_time != "unchanged" and annotation_status == "unchanged":
            # update only end-time
            qry = "UPDATE " + self.datastreamTable + " set end_time=%s where identifier=%s"
            vals = end_time, str(stream_id)
            isQueryReady = 1
        elif stream_end_time == "unchanged" and annotation_status == "changed":
            # update only annotations
            qry = "UPDATE " + self.datastreamTable + " set annotations=JSON_ARRAY_APPEND(annotations, '$.annotations',  CAST(%s AS JSON)) where identifier=%s"
            vals = json.dumps(annotations, default=str), str(stream_id)
            isQueryReady = 1

        elif (annotation_status == "new"):
            qry = "INSERT INTO " + self.datastreamTable + " (identifier, owner, name, data_descriptor, execution_context, annotations, type, start_time, end_time) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            vals = str(stream_id), str(owner_id), str(stream_name), json.dumps(
                data_descriptor), json.dumps(execution_context, default=str), json.dumps(
                annotations, default=str), stream_type, start_time, end_time
            isQueryReady = 1
            # if nothing is changed then isQueryReady would be 0 and no database transaction would be performed
        if isQueryReady == 1:
            try:
                self.execute(qry, vals, commit=True)
            except:
                self.logging.log(
                    error_message="Query: " + str(qry) + " - cannot be processed. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)

    def annotations_status(self, stream_id: uuid, owner_id: uuid, annotations: dict) -> str:
        """
        This method will check whether the stream already exist with the same data (as provided in params) except annotations.
        :param stream_id:
        :param owner_id:
        :param annotations:
        :return:
        """
        qry = "select annotations from " + self.datastreamTable + " where identifier = %s and owner=%s"
        vals = stream_id, owner_id
        result = self.execute(qry, vals)

        if result:
            if json.loads(result[0]["annotations"]) == annotations:
                return "unchanged"
            else:
                return "changed"
        else:
            return "new"

    def check_end_time(self, stream_id: uuid, end_time: datetime):
        """
        Check whether end time was changed of a stream-id
        :param stream_id:
        :param end_time:
        :return: It returns a datetime object if end-time was changed. Otherwise, it returns status as unchanged
        :rtype: str OR datetime
        """
        localtz = timezone(self.time_zone)

        qry = "SELECT end_time from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}
        rows = self.execute(qry, vals)

        if rows:
            old_end_time = rows[0]["end_time"]
            if end_time.tzinfo is None:
                end_time = localtz.localize(end_time)
            if old_end_time.tzinfo is None:
                old_end_time = localtz.localize(old_end_time)

            if old_end_time <= end_time:
                return end_time
            else:
                return "unchanged"
        else:
            self.logging.log(
                error_message="STREAM ID: " + stream_id + " - No record found. " + str(traceback.format_exc()),
                error_type=self.logtypes.DEBUG)

    def update_start_time(self, stream_id: uuid, new_start_time: datetime):
        """
        update start time only if the new-start-time is older than the existing start-time
        :param stream_id:
        :param new_start_time:
        :return:
        """
        localtz = timezone(self.time_zone)

        qry = "SELECT start_time from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}
        rows = self.execute(qry, vals)

        if rows:
            old_start_time = rows[0]["start_time"]
            if new_start_time.tzinfo is None:
                new_start_time = localtz.localize(new_start_time)
            if old_start_time.tzinfo is None:
                old_start_time = localtz.localize(old_start_time)

            if old_start_time > new_start_time:
                qry = "UPDATE " + self.datastreamTable + " set start_time=%s where identifier=%s"
                vals = new_start_time, str(stream_id)
                self.execute(qry, vals, commit=True)
        else:
            self.logging.log(
                error_message="STREAM ID: " + stream_id + " - No record found. " + str(traceback.format_exc()),
                error_type=self.logtypes.DEBUG)

    def mark_processed_day(self, owner_id: uuid, stream_id: uuid, day: str):
        """
        Mark row as processed if the data has been processed/ingested. This is used for data replay.
        :param owner_id:
        :param stream_id:
        :param day:
        """
        qry = "UPDATE " + self.dataReplayTable + " set processed=1 where owner_id=%s and stream_id=%s and day=%s"
        vals = str(owner_id), str(stream_id), str(day)
        self.execute(qry, vals, commit=True)

    def is_day_processed(self, owner_id: uuid, stream_id: uuid, day: str) -> bool:
        """
        Checks whether data is processed for a given user-id and stream-id
        :param owner_id:
        :param stream_id:
        :param day:
        :return: True if day is processed, False otherwise
        :rtype: bool
        """
        if day is not None:
            qry = "SELECT processed from " + self.dataReplayTable + " where owner_id=%s and stream_id=%s and day=%s"
            vals = str(owner_id), str(stream_id), str(day)
            rows = self.execute(qry, vals)
            if rows[0]["processed"] == 1:
                return True
            else:
                return False
        return False

    def get_replay_batch(self, record_limit: int = 5000) -> List:
        """
        This method helps in data replay. Yield a batch of data rows that needs to be processed and ingested in CerebralCortex
        :param record_limit:
        :return: List of dicts (keys=owner_id, stream_id, stream_name, day, files_list, metadata)
        :rtype: dict
        """
        good_participants = ['1c3e4944-1d23-4654-be10-c37b5806d00d', '1bdf0668-a632-4290-ad94-c6269f9e924a', '56b8d410-341c-4e90-ad7c-3e8bf1cbb0b6', '34521ae7-012a-400f-8794-3d76ff4e70ab', '3ca29402-0a0d-4f08-8f6c-5b57672809f6', 'f5abb4f1-ad31-4964-988c-14769501a8f7', 'a60bbc2d-4d45-4896-b533-15c2cd54cf60', '17b79ee0-4d3c-44ac-bfea-90b1f0540d4b', 'cdd98fba-4d2c-45d1-94b3-4b6b6077b58e', '16db9568-bd11-494f-b712-28a2266ea3d0', '94eb0755-56a3-4235-b759-8dc2ced70875', '5b1ab5af-701b-4717-9c43-98ab90a89325', 'bb23dbbc-b679-4849-b1d8-63279cad50e2', 'cd6f425b-92fe-4bdf-b277-53a558fc7c27', '44e31af4-7a40-4124-8dc4-d14b12dd66c7', 'c87dcc5b-2846-4eca-b6c2-ecc7ea58bbc7', 'c9b351d9-124b-4119-8be8-cb7c0f7e7994', '900b0dc1-b236-4364-bbab-0371af4eef84', 'b64eaa81-425f-446a-a521-9fdd9429f77b', '51ebb070-7d34-40b3-8520-55d2e318438a', 'ec7bb904-196c-4040-997e-d5e23ad1a553', '03996723-2411-4167-b14b-eb11dfc33124', 'd609008d-6efb-4cd0-ab84-6c0de55198db', '36f6e239-816a-4508-9126-3b612741c26d', 'f4aafd07-9711-4850-a6b6-63efa2fe25c6', 'ac9b4778-0bb5-4384-b4ed-3a5738ba99a4', 'ac3c89bc-11bb-447a-b226-5a4a935e9653', '95a070a7-086b-4f3d-a5ba-0a877f7fabf7', 'e44afc25-08b9-432e-ba89-7f0fc80b95cf', '9b7483ba-9c6f-4c67-bf48-549384ac66c7', '03c26210-7c9f-4bf2-b1c2-59d0bd64ffac', '94291262-3794-449e-a4c8-f62de9529977', '28fba926-ef44-4874-a209-ac6680441822', '342d9d87-787d-4836-9f61-4d59ed9f3289', '9324c6d7-3fdc-4614-8199-0c102f1b67c0', 'e7500981-9b13-4238-a855-52b91ed6244d', '6bf8a6da-a8c3-45c4-8aa3-75649cd1772d', '055bed5b-60ec-43e6-9110-137f2a36d65b', '135c9c3b-a5cf-47a4-9fcf-4fc418c5eb96', '843651b8-938f-4b33-9e5e-58a00a568c59', '4e13c9d7-58c9-4ab5-b111-f5c0a8ed05e2', '3b9ff2e4-dfec-4022-8994-1a0c4db7227a', '21e618f1-2cc1-4179-b001-62b05b3b1f7e', '3833df3d-dc81-4467-bdd9-16a7d99f7edb', '1b524925-07d8-42ea-8876-ada7298369ec', '63f5e202-6b13-491a-bdfc-9f13b7e4c036', '36caadf6-9bd5-4bac-9f13-75d2f439b4de', 'c73670a9-16ca-43bb-b7b9-110558e31798', 'f611477f-7b2e-4f36-81da-c6cdee27d7a1', '55675812-1eac-44e0-a57d-30a5a9ae083e', 'c696b0e8-299f-4270-9218-25f973bc64b4', '351fbcd3-c1ec-416c-bed7-195fe5d1f41d', '50846a75-cde7-44d0-878d-bedc39726f75', '0a11d9aa-1e9c-4f5e-94cf-faa6e796a855', '35daf881-da7c-4779-a8e9-20a3985094e2', '274739ff-aa7f-4b93-a409-67217327140c', 'df2b2506-6a64-4f94-8b7c-171a373387a3', '37733a30-f84c-416d-977f-ac3a5b2a68c4', 'f77f3c8b-49e6-44fe-92a1-c0b07bbea9e9', 'c5677eca-f00e-45af-ab0c-7388438c85e3', '059a9d92-4d36-40cb-84cc-408f9210821b', '805f3a7b-a197-4834-a4e4-a56da3dde6b1', '8cd3bb5b-32f7-4275-b789-bbcd7de0ee53', 'c6574c0d-ceca-4584-af55-d8e7e282ed8d']

        blacklist_regex = self.config["blacklist"]
        regex_cols = ""  # regex match on columns
        like_cols = ""  # like operator on columns
        for breg in blacklist_regex["regzex"]:
            regex_cols += '%s NOT REGEXP "%s" and ' % ("stream_name", blacklist_regex["regzex"][breg])

        for btm in blacklist_regex["txt_match"]:
            like_cols += '%s not like "%s" and ' % ("stream_name", blacklist_regex["txt_match"][btm])

        if regex_cols != "" and like_cols != "":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from " + self.dataReplayTable + " where " + regex_cols + " " + like_cols + "  processed=0"
        elif regex_cols != "" and like_cols == "":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from " + self.dataReplayTable + " where " + re.sub(
                "and $", "", regex_cols) + "  processed=0"
        elif regex_cols == "" and like_cols != "":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from " + self.dataReplayTable + " where " + re.sub(
                "and $", "", like_cols) + "  processed=0"
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
                    if row["owner_id"] in good_participants:
                        msgs.append(
                            {"owner_id": row["owner_id"], "stream_id": row["stream_id"], "stream_name": row["stream_name"],
                             "metadata": json.loads(row["metadata"]), "day": row["day"],
                             "filename": json.loads(row["files_list"])})
                yield msgs
            else:
                yield []
        else:
            yield []
