# Copyright (c) 2020, MD2K Center of Excellence
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

import os

import yaml
from pathlib import Path


class ConfigHandler:

    def load_file(self, filepath: str, default_configs=False):
        """
        Helper method to load a yaml file

        Args:
            filepath (str): path to a yml configuration file

        """

        with open(filepath, 'r') as ymlfile:
            self.config = yaml.safe_load(ymlfile)

        if default_configs:
            user_home_dir  = str(Path.home()) + "/cc_data/"
            self.config["filesystem"]["filesystem_path"] = user_home_dir
            Path(self.config["filesystem"]["filesystem_path"]).mkdir(parents=True, exist_ok=True)
            self.config["sqlite"]["file_path"] = user_home_dir
            self.config["cc"]["log_files_path"]  = user_home_dir +"logs/"
            Path(self.config["cc"]["log_files_path"]).mkdir(parents=True, exist_ok=True)

        if "hdfs" in self.config and self.config["hdfs"]["raw_files_dir"]!="" and self.config["hdfs"]["raw_files_dir"][-1] !="/":
            self.config["hdfs"]["raw_files_dir"]+="/"

        if "filesystem" in self.config and self.config["filesystem"]["filesystem_path"]!="" and self.config["filesystem"]["filesystem_path"][-1] !="/":
            self.config["filesystem"]["filesystem_path"]+="/"

        if "sqlite" in self.config and self.config["sqlite"]["file_path"]!="" and self.config["sqlite"]["file_path"][-1] !="/":
            self.config["sqlite"]["file_path"]+="/"

        if "log_files_path" in self.config and self.config["cc"]["log_files_path"]!="" and self.config["cc"]["log_files_path"][-1]!="":
            self.config["cc"]["log_files_path"] +="/"
            if not os.access(self.config["cc"]["log_files_path"], os.W_OK):
                raise Exception(self.config["cc"]["log_files_path"]+" path is not writable. Please check your cerebralcortex.yml configurations for 'log_files_path'.")

