# Setsum

Implementation of the [setsum algorithm] for unordered checksum of elements.

This is somewhat similar to [ECHM] for multiset hashing. 

## Performance

Can do around 66 million inserts per second on a 8 year old i7.

The implementation started out doing around 6 million. The 10x improvement
came from various c# optimizations and using SIMD.

[setsum algorithm]: https://github.com/rescrv/blue/tree/main/setsum
[ECHM]: https://github.com/arj03/ecmh-cs
