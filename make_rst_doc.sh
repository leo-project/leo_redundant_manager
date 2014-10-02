#!/bin/sh

rm -rf doc/rst && mkdir doc/rst
make doc
pandoc --read=html --write=rst doc/leo_cluster_tbl_conf.html -o doc/rst/leo_cluster_tbl_conf.rst
pandoc --read=html --write=rst doc/leo_cluster_tbl_member.html -o doc/rst/leo_cluster_tbl_member.rst
pandoc --read=html --write=rst doc/leo_cluster_tbl_ring.html -o doc/rst/leo_cluster_tbl_ring.rst
pandoc --read=html --write=rst doc/leo_cluster_tbl_sync_behaviour.html -o doc/rst/leo_cluster_tbl_sync_behaviour.rst
pandoc --read=html --write=rst doc/leo_mdcr_tbl_cluster_info.html -o doc/rst/leo_mdcr_tbl_cluster_info.rst
pandoc --read=html --write=rst doc/leo_mdcr_tbl_cluster_member.html -o doc/rst/leo_mdcr_tbl_cluster_member.rst
pandoc --read=html --write=rst doc/leo_mdcr_tbl_cluster_mgr.html -o doc/rst/leo_mdcr_tbl_cluster_mgr.rst
pandoc --read=html --write=rst doc/leo_mdcr_tbl_cluster_stat.html -o doc/rst/leo_mdcr_tbl_cluster_stat.rst
pandoc --read=html --write=rst doc/leo_mdcr_tbl_sync.html -o doc/rst/leo_mdcr_tbl_sync.html
pandoc --read=html --write=rst doc/leo_membership_cluster_local.html -o doc/rst/leo_membership_cluster_local.rst
pandoc --read=html --write=rst doc/leo_membership_cluster_remote.html -o doc/rst/leo_membership_cluster_remote.rst
pandoc --read=html --write=rst doc/leo_membership_mq_client.html -o doc/rst/leo_membership_mq_client.rst
pandoc --read=html --write=rst doc/leo_redundant_manager.html -o doc/rst/leo_redundant_manager.rst
pandoc --read=html --write=rst doc/leo_redundant_manager_api.html -o doc/rst/leo_redundant_manager_api.rst
pandoc --read=html --write=rst doc/leo_redundant_manager_chash.html -o doc/rst/leo_redundant_manager_chash.rst
pandoc --read=html --write=rst doc/leo_redundant_manager_worker.html -o doc/rst/leo_redundant_manager_worker.rst
pandoc --read=html --write=rst doc/leo_ring_tbl_transformer.html -o doc/rst/leo_ring_tbl_transformer.rst
