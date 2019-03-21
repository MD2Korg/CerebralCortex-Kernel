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

import pathlib
import re


def dir_scanner(dir_path: str, data_file_extension: list = [], allowed_filename_pattern: str = None,
                get_dirs: bool = False):
    """
    Generator method to iterate over directories and return file/dir

    Args:
        dir_path (str): path of main directory that needs to be iterated over
        data_file_extension (list): file extensions that must be excluded during directory scanning
        allowed_filename_pattern (str): regex expression to get file names matched to the regex
        get_dirs (bool): set it true to get directory name as well

    Yields:
        filename with its full path
    """
    dir_path = pathlib.Path(dir_path)
    if get_dirs:
        yield dir_path
    for sub in dir_path.iterdir():
        if sub.is_dir():
            yield from dir_scanner(sub, data_file_extension)
        else:
            if len(data_file_extension) > 0 and sub.suffix in data_file_extension:
                if allowed_filename_pattern is not None:
                    try:
                        allowed_filename_pattern = re.compile(allowed_filename_pattern)
                        if allowed_filename_pattern.search(sub._str):
                            yield sub._str
                    except re.error:
                        raise re.error
                else:
                    yield sub._str
