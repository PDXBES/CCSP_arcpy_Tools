from businessclasses.config import Config
from businessclasses.dme_master_hybrid import DmeMasterHybrid
from businessclasses.dme_master_link import DmeMasterLink
from dataio.dme_master_hybrid_db_data_io import DmeMasterHybridDbDataIo
import time

start = time.time()

config = Config('TEST')
dme_master_hybrid = DmeMasterHybrid(config)
dme_master_hybrid_db_data_io = DmeMasterHybridDbDataIo(config)

start = time.time()
test_dme_pipes = dme_master_hybrid.create_dme_links(dme_master_hybrid_db_data_io)
print time.time() - start

start = time.time()
test_masterlinks = dme_master_hybrid.create_master_links(dme_master_hybrid_db_data_io)
print time.time() - start

dme_master_links = []
for dme_link in test_dme_pipes:
    dme_master_link = dme_master_hybrid_db_data_io.create_object_with_id(DmeMasterLink)
    dme_master_link.global_id = dme_link.global_id
    dme_master_link.geometry = dme_link.geometry
    dme_master_links.append(dme_master_link)

dme_master_hybrid.append_dme_master_links_to_db(dme_master_links,
                                                dme_master_hybrid_db_data_io)
pass
