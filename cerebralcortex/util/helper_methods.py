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

import os
from typing import List

import pyarrow as pa

from cerebralcortex.core.config_manager.config import Configuration


def get_study_names(configs_dir_path: str=None, default_config=True)->List[str]:
    """
    CerebralCortex constructor

    Args:
        configs_dir_path (str): Directory path of cerebralcortex configurations.
    Returns:
        list(str): list of study names available
    Raises:
        ValueError: If configuration_filepath is None or empty.
    Examples:
        >>> get_study_names("/directory/path/of/configs/")
    """
    if default_config:
        config = Configuration(configs_dir_path, "default").config
    else:
        config = Configuration(configs_dir_path).config


    nosql_store = config['nosql_storage']

    if nosql_store == "hdfs":
        fs = pa.hdfs.connect(config['hdfs']['host'], config['hdfs']['port'])
        study_path = config['hdfs']['raw_files_dir']
        study_names = []
        all_studies = fs.ls(study_path)
        for strm in all_studies:
            study_names.append(strm.replace(study_path,"").replace("study=",""))
        return study_names
    elif nosql_store=="filesystem":
        filesystem_path = config["filesystem"]["filesystem_path"]
        if not os.access(filesystem_path, os.W_OK):
            raise Exception(filesystem_path+" path is not writable. Please check your cerebralcortex.yml configurations.")
        return [d.replace("study=","") for d in os.listdir(filesystem_path) if os.path.isdir(os.path.join(filesystem_path, d))]
    else:
        raise ValueError(nosql_store + " is not supported.")