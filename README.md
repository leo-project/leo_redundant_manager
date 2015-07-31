# leo_redundant_manager

[![Build Status](https://secure.travis-ci.org/leo-project/leo_redundant_manager.png?branch=develop)](http://travis-ci.org/leo-project/leo_redundant_manager)

## Overview

* "leo_redundant_manager" monitors Gateway-node(s) and Storage-node(s) for keeping availability and consistency.
* "leo_redundant_manager" manages and provide routing-table(RING).
* "leo_redundant_manager" uses [rebar](https://github.com/rebar/rebar) build system. Makefile so that simply running "make" at the top level should work.
* "leo_redundant_manager" requires Erlang R16B03-1 or later.

## Usage in Leo Project

**leo_redundant_manager** is used in [**leo_storage**](https://github.com/leo-project/leo_storage), [**leo_gateway**](https://github.com/leo-project/leo_gateway) and [**leo_manager**](https://github.com/leo-project/leo_manager)
It is used to distribute a routing table and manage members of a cluster.

## Sponsors

LeoProject/LeoFS is sponsored by [Rakuten, Inc.](http://global.rakuten.com/corp/) and supported by [Rakuten Institute of Technology](http://rit.rakuten.co.jp/).