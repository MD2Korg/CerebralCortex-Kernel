from cerebralcortex import Kernel
import json
import pandas as pd
from cerebralcortex.data_importer.directory_scanners import dir_scanner
from cerebralcortex.data_importer.metadata_parsers.mcerebrum import parse_mcerebrum_metadata, metadata_parser
from cerebralcortex.data_importer.raw_data_parsers.mcerebrum import mcerebrum_data_parser, assign_column_names_types
from cerebralcortex.core.data_manager.sql.data import SqlData

# CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", enable_spark=False)
# metadata_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"
# data_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"
#
# sql_data = SqlData(CC)


def import_data(data_dir, skip_file_extensions=[], batch_size=2, compression=None, header=None, json_parser=None, data_parser=None):
    metadata = None
    all_files = dir_scanner(data_dir, skip_file_extensions)
    for file_path in all_files:
        if json_parser is not None:
            metadata_file = file_path.replace(".gz", ".json")
            if metadata_file.endswith(".json"):
                with open(metadata_file, "r") as md:
                    metadata = md.read()
                    metadata = metadata.lower()
                    metadata = json.loads(metadata)
            else:
                pass #TODO: generate dynamic metadata
        if compression is not None:
            df = pd.read_fwf(file_path, compression=compression, header=header, quotechar='"')
        else:
            df = pd.read_fwf(file_path, header=header, quotechar='"')

        df = df.apply(data_parser, axis=1)
        df = assign_column_names_types(df, metadata)
        if json_parser is not None:
            metadata = metadata_parser(json_parser, metadata, df)


        print("Done")

import_data(data_dir="/home/ali/IdeaProjects/MD2K_DATA/data/test/",
            batch_size=2,
            compression='gzip',
            header=None,
            skip_file_extensions=[".json"],
            data_parser=mcerebrum_data_parser,
            json_parser=parse_mcerebrum_metadata)