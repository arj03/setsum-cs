# Setsum

Implementation of the [setsum algorithm] for unordered checksum of elements.

This is somewhat similar to [ECHM] for multiset hashing. 

## Performance

Can do around 66 million inserts per second on a 8 year old i7.

The implementation started out doing around 6 million. The 10x improvement
came from various c# optimizations and using SIMD.

## WASM

There is a WASM implementation in SetsumWASM written in
AssemblyScript. It can do 111 million inserts per second, so is faster
than c#. The compiled wasm file is 1.7KB.

## Sync

There is a unidirectional sync implementation between two nodes [here](SetSum/Sync).

[setsum algorithm]: https://github.com/rescrv/blue/tree/main/setsum
[ECHM]: https://github.com/arj03/ecmh-cs
