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


import inspect
import syslog
from datetime import datetime


class LogTypes():
    EXCEPTION = 1,
    CRITICAL = 2,
    ERROR = 3,
    WARNING = 4,
    MISSING_DATA = 5,
    DEBUG = 6


class LogHandler():
    def logsyslog(self, loglevel: str, message: str):
        """
        Initialize logging

        Args:
            loglevel (str): log level (e.g., warning, critical etc.)
            message (str): log message
        """
        syslog.openlog(ident="CerebralCortex")
        syslog.syslog(loglevel, message)
        syslog.closelog()

    def log(self, error_message: str = "", error_type=LogTypes.EXCEPTION):
        """
        Log errors and warnings in log file and print on console of debug is set to True

        Args:
            error_message (str): error message
            error_type (LogTypes): error type (e.g., warning, critical etc.)
        """
        execution_stats = inspect.stack()
        method_name = execution_stats[1][3]
        file_name = execution_stats[1][1]
        line_number = execution_stats[1][2]

        error_message = str(datetime.now()) + " - [" + str(file_name) + " - " + str(method_name) + " - " + str(
            line_number) + "] - " + str(error_message)

        if error_type == LogTypes.CRITICAL:
            self.logsyslog(syslog.LOG_CRIT, error_message)
        elif error_type == LogTypes.ERROR:
            self.logsyslog(syslog.LOG_ERR, error_message)
        elif error_type == LogTypes.EXCEPTION:
            self.logsyslog(syslog.LOG_ERR, error_message)
        elif error_type == LogTypes.WARNING:
            self.logsyslog(syslog.LOG_WARNING, error_message)
        elif error_type == LogTypes.DEBUG:
            self.logsyslog(syslog.LOG_DEBUG, error_message)
        elif error_type == LogTypes.MISSING_DATA:
            error_message = 'MISSING_DATA ' + error_message
            self.logsyslog(syslog.LOG_ERR, error_message)
        else:
            self.logsyslog(syslog.LOG_INFO, error_message)

        if self.debug:
            print(error_message)

        if self.throw_exception:
            raise Exception(error_message)
