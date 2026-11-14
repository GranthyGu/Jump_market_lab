# CMU BusTub B+ tree adaptation

Upstream: https://github.com/cmu-db/bustub
Commit: `e42a66f171093c138464b8302f5c3677a08e2ce4`
License: MIT (see LICENSE).

Only the buffer pool, page guards, disk scheduler and B+ tree subset is vendored.
The upstream teaching skeleton was completed locally. Additional behavior:
4-byte duplicate user keys, value tie breakers in internal routing, reopening an
existing tree, and nonblocking sibling latching during scans. The default unique
key mode retains the upstream test API, including tombstones and removal.

This directory is the source of truth. Official tests live in a temporary upstream
checkout under build/, and compile these source files. Do not edit that checkout.

The MarketLab CMake target defines `MARKETLAB_BPT_STANDALONE=1`: trade types are
instantiated from the template headers, without BusTub's database dependencies.
The four explicit-instantiation `.cpp` files also belong to that target so editor
tools receive its configuration. Their `GenericKey`/`RID` instantiations are only
enabled in the full upstream build, where those types and debug helpers exist.
