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
