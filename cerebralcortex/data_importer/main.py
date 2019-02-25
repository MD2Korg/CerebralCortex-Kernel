from cerebralcortex import Kernel
import json
from cerebralcortex.data_importer.directory_scanners import dir_scanner
from cerebralcortex.data_importer.metadata_parsers.mcerebrum import parse_mcerebrum_metadata
from cerebralcortex.data_importer.raw_data_parsers.mcerebrum import parse_mcerebrum_data
from cerebralcortex.core.data_manager.sql.data import SqlData

CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", enable_spark=False)
metadata_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"
data_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"

sql_data = SqlData(CC)

data_dir = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"
g = dir_scanner(data_dir)
#[next(g) for _ in range(10)]

for file_path in g:
    metadata_file = file_path.replace(".gz", ".json")
    if metadata_file.endswith(".json"):
        with open(metadata_file, "r") as md:
            metadata = md.read()
            metadata = metadata.lower()
            metadata = json.loads(metadata)

    df = parse_mcerebrum_data(file_path, metadata, "112233")
    metadata = parse_mcerebrum_metadata(file_path, df)


    print("Done")