# Copyright (c) 2017, MD2K Center of Excellence
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
import re
from datetime import datetime, timedelta
from typing import List
import traceback
from pytz import timezone

from cerebralcortex.core.util.debuging_decorators import log_execution_time


class StreamHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_stream_metadata(self, stream_id: uuid) -> dict:
        """

        :param stream_id:
        :return:
        """
        qry = "SELECT * from " + self.datastreamTable + " where identifier=%(identifier)s"
        vals = {"identifier": str(stream_id)}
        rows = self.execute(qry, vals)
        return rows

    def get_stream_metadata_by_user(self, user_id: uuid, stream_name: str = None, start_time: datetime = None,
                                     end_time: datetime = None) -> List:
        """
        Returns all the stream ids and name that belongs to an owner-id
        :param user_id:
        :return:
        """
        stream_ids_names = {}
        vals = []
        if not user_id:
            print("User ID cannot be empty/None.")
            return None

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

        qry = qry+where_clause
        vals = tuple(vals)
        rows = self.execute(qry, vals)
        return rows

    def user_has_stream(self, user_id: uuid, stream_name: str) ->bool:
        """
        Returns true if a user has a stream available
        :param user_id: 
        :param stream_name: 
        :return: 
        """
        if not stream_name or not user_id:
            return []

        qry = "select identifier from " + self.datastreamTable + " where owner=%s and name = %s"
        vals = str(user_id),str(stream_name)

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return False
        else:
            return True
    
    def get_stream_duration(self, stream_id: uuid) -> dict:
        """

        :param stream_id:
        :return:
        """
        if not stream_id:
            return None

        qry = "select start_time, end_time from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return {"start_time": None, "end_time": None}
        else:
            return {"start_time": rows[0]["start_time"], "end_time": rows[0]["end_time"]}

    def get_all_users(self, study_name: str) -> List:

        """

        :param study_name:
        :return:
        """
        if not study_name:
            print("Study name cannot be empty.")
            return []
        results = []
        qry = 'SELECT identifier, username FROM ' + self.userTable + ' where user_metadata->"$.study_name"=%(study_name)s'
        vals = {'study_name': str(study_name)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            print("No record found - get_all_users")
            return []
        else:
            for row in rows:
                results.append(row)
            return results

    def get_user_streams(self, user_id: uuid) -> dict:

        """

        :param user_id:
        :return:
        """
        if not user_id:
            print("User ID cannot be empty.")
            return {}
        result = {}
        qry = 'SELECT * FROM ' + self.datastreamTable + ' where owner=%(owner)s'
        vals = {'owner': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return {}
        else:
            for row in rows:
                stream_ids = self.get_stream_id(str(user_id), row["name"])
                row["stream_ids"] = [d['identifier'] for d in stream_ids]
                result[row["name"]] = row
            return result

    def get_user_streams_metadata(self, user_id: str) -> dict:
        """

        :param user_id:
        :return:
        """
        if not user_id:
            return {}
        result = {}
        qry = "select name, data_descriptor,execution_context,annotations, start_time, end_time from " + self.datastreamTable + " where owner = %(owner)s"
        vals = {'owner': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return {}
        else:
            for row in rows:
                stream_ids = self.get_stream_id(str(user_id), row["name"])
                row["stream_ids"] = [d['identifier'] for d in stream_ids]
                result[row["name"]] = row
            return result

    def get_user_name(self, user_id: uuid) -> str:
        """

        :param user_id:
        :return:
        """
        if not user_id:
            return None

        qry = "select username from " + self.userTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(user_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["username"]
        
    def is_user(self, user_id: uuid=None, user_name:uuid=None) -> bool:
        """

        :param user_id:
        :return:
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
            print("Wrong parameters.")
            return False

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return False
        else:
            return True

    def get_user_id(self, user_name: str) -> str:
        """

        :param user_name:
        :return:
        """
        if not user_name:
            return None

        qry = "select identifier from " + self.userTable + " where username = %(username)s"
        vals = {'username': str(user_name)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["identifier"]

    def get_stream_id(self, user_id:uuid, stream_name: str) -> List:
        """
        :param user_id
        :param stream_name:
        :return:
        """
        if not stream_name or not user_id:
            return []

        qry = "select identifier from " + self.datastreamTable + " where owner=%s and name = %s"
        vals = str(user_id),str(stream_name)

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return []
        else:
            return rows

    def get_stream_days(self, stream_id: uuid) -> List:
        """
        Returns a list of days (string format: YearMonthDay (e.g., 20171206) for a given stream-id
        :param stream_id:
        :param dd_stream_id:
        """
        all_days = []
        stream_days = self.get_stream_duration(stream_id)
        days = stream_days["end_time"]-stream_days["start_time"]
        for day in range(days.days+1):
            all_days.append((stream_days["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))

        return all_days

    def get_stream_name(self, stream_id: uuid) -> str:
        """

        :param stream_id:
        :return:
        """
        if not stream_id:
            print("Stream ID cannot be empty.")
            return ""

        qry = "select name from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return ""
        else:
            return rows[0]["name"]

    def is_stream(self, stream_id: uuid)->bool:

        """

        :param stream_id:
        :return:
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
        :param owner_id:
        :param stream_name:
        :param data_descriptor:
        :param execution_context:
        :param annotations:
        :param stream_type:
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
                self.logging.log(error_message="Query: "+str(qry)+" - cannot be processed. "+str(traceback.format_exc()), error_type=self.logtypes.CRITICAL)
                
    def annotations_status(self, stream_id: uuid, owner_id: uuid, annotations: dict) -> str:
        """
        This method will check if the stream already exist with the same data (as provided in params) except annotations.
        :param stream_id:
        :param owner_id:
        :param name:
        :param data_descriptor:
        :param execution_context:
        :param stream_type:
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

    def check_end_time(self, stream_id: uuid, end_time: datetime) -> str:
        """

        :param stream_id:
        :param end_time:
        :return:
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
            self.logging.log(error_message="STREAM ID: "+stream_id+" - No record found. "+str(traceback.format_exc()), error_type=self.logtypes.DEBUG)

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
            self.logging.log(error_message="STREAM ID: "+stream_id+" - No record found. "+str(traceback.format_exc()), error_type=self.logtypes.DEBUG)


    def mark_processed_day(self, owner_id, stream_id, day):
        qry = "UPDATE "+self.dataReplayTable+" set processed=1 where owner_id=%s and stream_id=%s and day=%s"
        vals = str(owner_id), str(stream_id), str(day)
        self.execute(qry, vals, commit=True)

    def is_day_processed(self, owner_id, stream_id, day)->bool:
        if day is not None:
            qry = "SELECT processed from "+self.dataReplayTable+" where owner_id=%s and stream_id=%s and day=%s"
            vals = str(owner_id), str(stream_id), str(day)
            rows = self.execute(qry, vals)
            if rows[0]["processed"]==1:
                return True
            else:
                return False
        return False

    def get_replay_batch(self, record_limit:int=5000)-> List:
        blacklist_regex = self.config["blacklist"]
        regex_cols = "" #regex match on columns
        like_cols = "" # like operator on columns
        for breg in blacklist_regex["regzex"]:
            regex_cols += '%s NOT REGEXP "%s" and ' % ("stream_name", blacklist_regex["regzex"][breg])

        for btm in blacklist_regex["txt_match"]:
            like_cols += '%s not like "%s" and ' % ("stream_name", blacklist_regex["txt_match"][btm])

        if regex_cols!="" and like_cols!="":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from "+self.dataReplayTable+" where " + regex_cols +" "+like_cols+"  processed=0"
        elif regex_cols!="" and like_cols=="":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from "+self.dataReplayTable+" where " + re.sub("and $", "", regex_cols)+"  processed=0"
        elif regex_cols=="" and like_cols!="":
            qry = "SELECT owner_id, stream_id, stream_name, day, files_list, metadata from "+self.dataReplayTable+" where " + re.sub("and $", "", like_cols)+"  processed=0"
        else:
            qry = ""

        if qry!="":
            rows = self.execute(qry)
            msgs = []
            if len(rows)>0:
                for row in rows:
                    if len(msgs)>int(record_limit):
                        yield msgs
                        msgs = []
                    msgs.append({"owner_id":row["owner_id"], "stream_id":row["stream_id"],"stream_name":row["stream_name"], "metadata": json.loads(row["metadata"]), "day":row["day"], "filename":json.loads(row["files_list"])})
                yield msgs
            else:
                yield []
        else:
            yield []