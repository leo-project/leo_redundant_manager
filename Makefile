.PHONY: all get_deps comile xref eunit check_plt build_plt dialyzer doc callgraph graphviz clean distclean

REBAR := ./rebar
APPS = erts kernel stdlib sasl crypto compiler inets mnesia public_key runtime_tools snmp syntax_tools tools xmerl webtool ssl
LIBS = deps/leo_commons/ebin deps/leo_mq/ebin deps/leo_rpc/ebin
PLT_FILE = .leo_redundant_manager_dialyzer_plt
COMMON_PLT_FILE = .common_dialyzer_plt
DOT_FILE = leo_redundant_manager.dot
CALL_GRAPH_FILE = leo_redundant_manager.png

all:
	@$(REBAR) get-deps
	@$(REBAR) compile
	@$(REBAR) xref skip_deps=true
	@$(REBAR) eunit suites=leo_mdcr_tbl_cluster_info
	@$(REBAR) eunit suites=leo_mdcr_tbl_cluster_member
	@$(REBAR) eunit suites=leo_mdcr_tbl_cluster_mgr
	@$(REBAR) eunit suites=leo_mdcr_tbl_cluster_stat
	@$(REBAR) eunit suites=leo_redundant_manager_worker
	@$(REBAR) eunit suites=leo_redundant_manager_api
	@$(REBAR) eunit suites=leo_membership_cluster_local
	@$(REBAR) eunit suites=leo_membership_mq_client
compile:
	@$(REBAR) compile
xref:
	@$(REBAR) xref skip_deps=true
eunit:
	@$(REBAR) eunit suites=leo_mdcr_tbl_cluster_info
	@$(REBAR) eunit suites=leo_mdcr_tbl_cluster_member
	@$(REBAR) eunit suites=leo_mdcr_tbl_cluster_mgr
	@$(REBAR) eunit suites=leo_mdcr_tbl_cluster_stat
	@$(REBAR) eunit suites=leo_redundant_manager_worker
	@$(REBAR) eunit suites=leo_redundant_manager_api
	@$(REBAR) eunit suites=leo_membership_cluster_local
	@$(REBAR) eunit suites=leo_membership_mq_client
check_plt:
	@$(REBAR) compile
	dialyzer --check_plt --plt $(PLT_FILE) --apps $(APPS)
build_plt:
	@$(REBAR) compile
	dialyzer --build_plt --output_plt $(PLT_FILE) --apps $(LIBS)
dialyzer:
	@$(REBAR) compile
	dialyzer -Wno_return --plts $(PLT_FILE) $(COMMON_PLT_FILE) -r ebin/ --dump_callgraph $(DOT_FILE) -Wrace_conditions | fgrep -v -f ./dialyzer.ignore-warnings
doc: compile
	@$(REBAR) doc
callgraph: graphviz
	dot -Tpng -o$(CALL_GRAPH_FILE) $(DOT_FILE)
graphviz:
	$(if $(shell which dot),,$(error "To make the depgraph, you need graphviz installed"))
clean:
	@$(REBAR) clean
distclean:
	@$(REBAR) delete-deps
	@$(REBAR) clean
