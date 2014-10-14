#!/bin/sh

make doc
rm -rf doc/rst && mkdir doc/rst

for Mod in leo_cluster_tbl_conf \
           leo_cluster_tbl_member \
           leo_cluster_tbl_ring \
           leo_mdcr_tbl_cluster_info \
           leo_mdcr_tbl_cluster_member \
           leo_mdcr_tbl_cluster_mgr \
           leo_mdcr_tbl_cluster_stat \
           leo_mdcr_tbl_sync \
           leo_membership_cluster_local \
           leo_membership_cluster_remote \
           leo_membership_mq_client \
           leo_redundant_manager \
           leo_redundant_manager_api \
           leo_redundant_manager_chash \
           leo_redundant_manager_worker \
           leo_ring_tbl_transformer
do
    read_file="doc/$Mod.html"
    write_file="doc/rst/$Mod.rst"

    pandoc --read=html --write=rst "$read_file" -o "$write_file"

    sed -ie "1,6d" "$write_file"
    sed -ie "1s/\Module //" "$write_file"
    LINE_1=`cat $write_file | wc -l`
    LINE_2=`expr $LINE_1 - 10`
    sed -ie "$LINE_2,\$d" "$write_file"
done
rm -rf doc/rst/*.rste
