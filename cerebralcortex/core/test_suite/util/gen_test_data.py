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

import gzip
import random
import argparse
from datetime import datetime
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.util.data_types import convert_sample


def gen_raw_data(filepath: str, row_size, get_data, dp_type):
    """
    write sample .gz data file
    :param filepath:
    """
    dps = get_datapoints(row_size, dp_type)
    with gzip.open(filepath, 'wb') as output_file:
        output_file.write(dps.encode())

    if get_data:
        return get_datapoints(row_size, dp_type)

def get_datapoints(dp_size: int, dp_type: str = "list") -> object:
    """
    Returns a list or string of sample data points
    :param dp_size: int
    :param dp_type: str or list
    :return:
    """
    if dp_type == "str":
        dps = "41,F8,00,00,3E,48,40,83,42,1C,00,00,40,97,3B,C9,3B,3F,6E,30,49,8F,4C,13,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,C0,43,E4,E6,C1,85,2E,48,00,00,00,1F,3E,24,94,06,3D,AD,5C,B5,3D,56,A2,06,3D,04,DE,50,3D,40,92,16,3D,69,DC,CC,3E,1B,C7,6C,3C,B7,8A,44,3C,3B,CF,26,3E,48,40,83,3C,10,A9,A8,3E,00,C2,5E,3D,2F,31,48,3C,90,A4,C8,3D,58,2E,28,3D,AC,51,8B,3D,CC,B3,25,3D,C2,CA,4E,3D,9D,30,CB,3C,F6,F5,4F,3C,FF,18,CA,3C,AB,78,F3,3D,22,81,56,3D,9A,F0,FD,3D,48,6F,DD,3C,11,2D,DC,3D,57,47,B1,3D,15,DA,32,3D,45,CB,D2,3C,00,54,04,3C,F8,CC,41,00,09,00,0D,00,13,00,15,00,19,00,1B,00,1E,00,21,00,24,00,27,00,2D,00,31,00,35,00,37,00,3A,00,3D,00,42,00,46,00,4C,00,50,00,52,00,56,00,5A,00,5E,00,61,00,64,00,67,00,6A,00,73,00,78,00,7B"
    else:
        dps = []
    for row in range(1, dp_size):
        sample = str(random.random()) + "," + str(random.random()) + "," + str(random.random()) + "," + str(random.random()) + "," + str(random.random())
        if row < 1000:
            tmp = 1519355691123  # 20180223
        elif row < 5000 and row > 1000:
            tmp = 1519255691123  # 20180221
        else:
            tmp = 1519455691123  # 20180224
        start_time = str(tmp + (row * 10))
        if dp_type == "str":
            dps += (start_time + ",-21600000," + str(sample) + "\n")
        else:
            dps.append(
                DataPoint(datetime.utcfromtimestamp(int(start_time) / 1000), None, -21600000, convert_sample(sample)))
    return dps


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample data to test CerebralCortex")
    parser.add_argument("-of", "--output_filepath", help="Output file path, e.g., /home/ali/test.gz", required=False)
    args = vars(parser.parse_args())
    if args["output_filepath"]:
        output_filepath = args["output_filepath"]
    else:
        output_filepath = "../test_data/raw/11111111-107f-3624-aff2-dc0e0b5be53d/20171122/00000000-107f-3624-aff2-dc0e0b5be53d/7b3538af-1299-4504-b8fd-62683c66578e.gz"
    gen_raw_data(output_filepath)
