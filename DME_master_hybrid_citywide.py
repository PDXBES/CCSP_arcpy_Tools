from businessclasses.config import Config
from businessclasses.dme_master_hybrid import DmeMasterHybrid
from businessclasses.dme_master_link import DmeMasterLink
from dataio.dme_master_hybrid_db_data_io import DmeMasterHybridDbDataIo
import time


    ## will write out result with citywide extent (DME_master_hybrid)
    ## 1. create in memory version of DME links
    ## 2. output is based on DME geometry
    ## 3. apply decision logic to choose between DME and master link values where global_ids match
    ## 4a. if copy - copy result to ETL input gdb
    ## 4b. if append - append result to CCSP.DME_MASTER_HYBRID

def create_citywide_hybrid():
    start = time.time()
    #config = Config('TEST')
    config = Config('PROD')
    dme_master_hybrid = DmeMasterHybrid(config)
    dme_master_hybrid_db_data_io = DmeMasterHybridDbDataIo(config)

    start = time.time()
    test_dme_links = dme_master_hybrid.create_dme_links(dme_master_hybrid_db_data_io)
    print("   Creating DME links - duration: " + str(time.time() - start) + " seconds")

    start = time.time()
    test_master_links = dme_master_hybrid.create_master_links(dme_master_hybrid_db_data_io)
    print("   Creating master links - duration: " + str(time.time() - start) + " seconds")

    dme_master_links = []
    start = time.time()
    test_dme_links.sort(key=lambda x: x.global_id, reverse=True)
    test_master_links.sort(key=lambda x: x.global_id, reverse=True)
    invalid_counter = 0

    for dme_link in test_dme_links:
        dme_master_link = DmeMasterLink(config)
        dme_master_link.global_id = dme_link.global_id
        dme_master_link.us_node_name = dme_link.us_node_name
        dme_master_link.ds_node_name = dme_link.ds_node_name
        dme_master_link.compkey = dme_link.compkey
        dme_master_link.diameter = dme_link.diameter
        dme_master_link.geometry = dme_link.geometry

        for index, master_link in enumerate(test_master_links):
            if dme_link.global_id == master_link.global_id:
                diameter = 0
                valid_us_depth = False
                valid_ds_depth = False
                valid_diameter = False
                valid_dme_diameter = False
                valid_master_diameter = False
                valid_ies = (master_link.us_ie != 0 and master_link.ds_ie != 0)
                if (dme_link.diameter > 0) and dme_link.diameter is not None:
                    valid_dme_diameter = True
                if (master_link.diameter > 0) and master_link.diameter is not None:
                    valid_master_diameter = True

                if valid_dme_diameter:
                    diameter = dme_link.diameter
                    valid_diameter = True
                elif valid_master_diameter:
                    diameter = master_link.diameter
                    valid_diameter = True
                else:
                    #print "         Invalid Diameter"
                    invalid_counter += 1
                    diameter = 4
                    valid_diameter = False

                if valid_diameter and valid_ies:
                    min_depth = (diameter / 12.0 + 1.0)
                    if master_link.us_depth > min_depth:
                        valid_us_depth = True
                    if master_link.ds_depth > min_depth:
                        valid_ds_depth = True

                if valid_us_depth and valid_ds_depth:
                    dme_master_link.us_depth = master_link.us_depth
                    dme_master_link.ds_depth = master_link.ds_depth
                    dme_master_link.us_ie = master_link.us_ie
                    dme_master_link.ds_ie = master_link.ds_ie
                    dme_master_link.link_id = master_link.link_id
                    dme_master_link.us_depth_source = "Master"
                    dme_master_link.ds_depth_source = "Master"
                else:
                    dme_master_link.us_depth = 10
                    dme_master_link.ds_depth = 10
                    if valid_us_depth:
                        dme_master_link.us_depth = master_link.us_depth
                        dme_master_link.us_depth_source = "Master"
                    else:
                        dme_master_link.us_depth = 10
                        dme_master_link.us_depth_source = "10_ft_fill_Master"
                    if valid_ds_depth:
                        dme_master_link.ds_depth = master_link.ds_depth
                        dme_master_link.ds_depth_source = "Master"
                    else:
                        dme_master_link.ds_depth = 10
                        dme_master_link.ds_depth_source = "10_ft_fill_Master"
                dme_master_link.diameter = diameter
                dme_master_links.append(dme_master_link)
                test_master_links.pop(index)
                break
        else:
            dme_master_link.us_depth = 10
            dme_master_link.ds_depth = 10
            dme_master_link.us_ie = None
            dme_master_link.ds_ie = None
            dme_master_link.link_id = None
            dme_master_link.us_depth_source = "10ftFill"
            dme_master_link.ds_depth_source = "10ftFill"
            dme_master_link.diameter = 4
            dme_master_links.append(dme_master_link)

    if invalid_counter > 0:
        print("   Records with Invalid Diameter: " + str(invalid_counter))
    print("   Data conditioning - duration: " + str(time.time() - start) + " seconds")

    start = time.time()
    dme_master_hybrid_db_data_io.append_dme_master_links_to_db(dme_master_links)
    #dme_master_hybrid_db_data_io.copy_dme_master_links_to_db(dme_master_links)
    print("   Writing result - duration: " + str(time.time() - start) + " seconds")
    pass

#FOR TESTING/ RUNNING INDEPENDENTLY
#create_citywide_hybrid()