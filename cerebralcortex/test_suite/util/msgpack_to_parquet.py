import gzip
import pandas as pd
import msgpack
import argparse

def msgpack_to_pandas(input_data: object) -> pd.DataFrame:
    """
    Convert msgpack binary file into pandas dataframe

    Args:
        input_data (msgpack): msgpack data file

    Returns:
        dataframe: pandas dataframe

    """
    data = []

    unpacker = msgpack.Unpacker(input_data, use_list=False, raw=False)
    for unpacked in unpacker:
        data.append(list(unpacked))

    header = data[0]
    data = data[1:]

    if data is None:
        return None
    else:
        df = pd.DataFrame(data, columns=header)
        df.columns = df.columns.str.lower()
        df.timestamp = pd.to_datetime(df['timestamp'], unit='us')
        df.timestamp = df.timestamp.dt.tz_localize('UTC')
        df.localtime = pd.to_datetime(df['localtime'], unit='us')
        df.localtime = df.localtime.dt.tz_localize('UTC')
        return df


if __name__=="__main__":

    parser = argparse.ArgumentParser(description="Read MessagePack File")
    parser.add_argument('-f', '--file_path', help='CC Configuration directory path')

    args = vars(parser.parse_args())
    file_path = str(args["file_path"]).strip()

    with gzip.open(file_path, 'rb') as input_data:
        data_frame = msgpack_to_pandas(input_data)


    print(data_frame)
