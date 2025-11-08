# Setsum

Implementation of the [setsum algorithm] for unordered checksum of elements.

This is somewhat similar to [ECHM] for multiset hashing. 

## Performance

Can do around 33 million inserts per second on a 8 year old i7.

Note it currently uses SHA2 instead of SHA3 because SHA3 is not available on Windows 10. 

[setsum algorithm]: https://github.com/rescrv/blue/tree/main/setsum
[ECHM]: https://github.com/arj03/ecmh-cs
