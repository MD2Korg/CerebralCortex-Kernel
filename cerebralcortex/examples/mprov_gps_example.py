from cerebralcortex.test_suite.util.data_helper import gen_location_datastream
from cerebralcortex.algorithms.gps.clustering import cluster_gps
from cerebralcortex.kernel import Kernel


CC = Kernel(cc_configs="default", mprov=True)
ds_gps = gen_location_datastream(user_id="bfb2ca0c-e19c-3956-9db2-5459ccadd40c", stream_name="gps--org.md2k.phonesensor--phone")

d2=ds_gps.window(windowDuration=60)
dd=cluster_gps(d2)
dd.show(1)