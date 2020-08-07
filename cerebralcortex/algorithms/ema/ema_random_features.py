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

import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType, TimestampType

schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("localtime", TimestampType()),
    StructField("user", StringType()),
    StructField("version", IntegerType()),
    StructField("name", StringType()),
    StructField("trigger_type", StringType()),
    StructField("start_time", TimestampType()),
    StructField("end_time", TimestampType()),
    StructField("total_time", FloatType()),
    StructField("total_questions", IntegerType()),
    StructField("total_answers", FloatType()),
    StructField("average_question_length", FloatType()),
    StructField("average_total_answer_options", FloatType()),
    StructField("time_between_ema", FloatType()),
    StructField("status", StringType()),
    StructField("question_answers", StringType())


])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def get_ema_random_features(user_data):
    all_vals = []
    time_between_ema = None
    for index, row in user_data.iterrows():
        ema = row['status']
        if not isinstance(ema, dict):
            ema = json.loads(ema)

        name = ema["name"]
        trigger_type = ema["trigger_type"]
        total_questions = len(ema.get("question_answers",[]))
        status = ema["status"]
        start_time = pd.to_datetime(ema["start_timestamp"], unit='ms')
        end_time = pd.to_datetime(ema["end_timestamp"], unit='ms')
        total_time = (end_time-start_time).total_seconds()
        question_length = []
        total_answers = 0
        total_answer_options = []
        average_question_length = 0.0
        average_total_answer_options = 0.0
        time_between_emas = 0.0
        question_answers_ = json.dumps(ema.get("question_answers",[]))
        for question in ema.get("question_answers",[]):
            response_option = question.get("response_option",[]) or []
            user_response = question.get("response",[]) or []
            question_text = question.get("question_text") or ""
            question_length.append(len(question_text))
            total_answer_options.append(len(response_option))

            if len(user_response)>0:
                total_answers +=1

        if time_between_ema is None:
            last_ema_time = start_time
            time_between_ema = 0.0
        else:
            time_between_ema = (start_time - last_ema_time).total_seconds()
            last_ema_time = start_time

        if len(question_length)>0:
            average_question_length = sum(question_length) / len(question_length)
        if len(question_length)>0:
            average_total_answer_options = sum(total_answer_options) / len(total_answer_options)

        all_vals.append([row["timestamp"],row["localtime"], row["user"],1,name,trigger_type,start_time,end_time,total_time,total_questions,total_answers,average_question_length,average_total_answer_options,time_between_ema,status,question_answers_])

    return pd.DataFrame(all_vals,columns=['timestamp','localtime', 'user', 'version','name','trigger_type','start_time','end_time','total_time','total_questions','total_answers', 'average_question_length','average_total_answer_options','time_between_ema','status','question_answers'])
