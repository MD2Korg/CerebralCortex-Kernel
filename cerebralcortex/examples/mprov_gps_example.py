from cerebralcortex.algorithms.gps.clustering import cluster_gps
from cerebralcortex.kernel import Kernel
from cerebralcortex.test_suite.util.data_helper import gen_location_datastream

cc_config = "/Users/ali/IdeaProjects/CerebralCortex-2.0/conf/"

# Create CC object
CC = Kernel(configs_dir_path=cc_config)

# get location data
ds_gps = gen_location_datastream(user_id="bfb2ca0c-e19c-3956-9db2-5459ccadd40c", stream_name="gps--org.md2k.phonesensor--phone")

# window location data
d2=ds_gps.window(windowDuration=60)

# Cluster GPS data
clusterz = cluster_gps(d2)

# Store results
CC.save_stream(clusterz)