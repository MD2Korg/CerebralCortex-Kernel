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

from cerebralcortex.core.data_manager.time_series.influxdb_handler import InfluxdbHandler
from cerebralcortex.core.log_manager.log_handler import LogTypes


class TimeSeriesData(InfluxdbHandler):
    def __init__(self, CC):
        """
        Constructor

        Args:
            CC (CerebralCortex): CerebralCortex object reference
        Raises:
            ValueError: visualization_storage param is set to none in cerebralcortex.yml. Please provide proper configuration for visualization storage.
        """
        self.configuration = CC.config

        self.sql_data = CC.SqlData

        self.logging = CC.logging
        self.logtypes = LogTypes()

        if self.configuration['visualization_storage']=="none":
            raise ValueError("visualization_storage param is set to none in cerebralcortex.yml. Please provide proper configuration for visualization storage.")

        self.influxdbIP = self.configuration['influxdb']['host']
        self.influxdbPort = self.configuration['influxdb']['port']
        self.influxdbDatabase = self.configuration['influxdb']['database']
        self.influxdbUser = self.configuration['influxdb']['db_user']
        self.influxdbPassword = self.configuration['influxdb']['db_pass']
