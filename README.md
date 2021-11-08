bit: big immutable table
========================

`bit` implements a fast immutable hashtable for a set of static key/value pairs that is robust to on-disk errors.
It is especially useful if that set of key value pairs is too big to fit in main memory.

Accessing a `*bit.Table` requires no locks or atomic operations, and is allocation free.
At `GOMAXPROCS=8` it is within a factor of two the speed of a Go `map[string]string` and 4x faster than `go-sparkey`:

```
# the `*bit.Table` in this project
BenchmarkBitGet                     	 4129772	       286.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkBitGet-2                   	 8114454	       145.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkBitGet-4                   	14859070	        79.44 ns/op	       0 B/op	       0 allocs/op
BenchmarkBitGet-8                   	30344920	        39.00 ns/op	       0 B/op	       0 allocs/op
# a standard Go map[string]string (not protected by a RWMutex or Lock)
BenchmarkMapGet                     	 9121705	       131.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkMapGet-2                   	17077434	        67.47 ns/op	       0 B/op	       0 allocs/op
BenchmarkMapGet-4                   	27612442	        43.49 ns/op	       0 B/op	       0 allocs/op
BenchmarkMapGet-8                   	54106759	        22.74 ns/op	       0 B/op	       0 allocs/op
# go-sparkey with sparkey a69925b2 from 2020-07-26 (latest as of writing)
BenchmarkSparkeyUncompressedGet     	 1490412	       777.1 ns/op	     592 B/op	      11 allocs/op
BenchmarkSparkeyUncompressedGet-2   	 2735966	       420.7 ns/op	     592 B/op	      11 allocs/op
BenchmarkSparkeyUncompressedGet-4   	 5095195	       231.5 ns/op	     592 B/op	      11 allocs/op
BenchmarkSparkeyUncompressedGet-8   	 7772685	       162.6 ns/op	     592 B/op	      11 allocs/op
# pure Go colinmarc/cdb
BenchmarkCdbGet                     	  672046	      1673 ns/op	     116 B/op	       3 allocs/op
BenchmarkCdbGet-2                   	  695947	      1686 ns/op	     116 B/op	       3 allocs/op
BenchmarkCdbGet-4                   	  751789	      1864 ns/op	     116 B/op	       3 allocs/op
BenchmarkCdbGet-8                   	  364162	      3330 ns/op	     115 B/op	       3 allocs/op
```

In particular, `bit` makes two large tradeoffs to achieve high `Get` performance.
First, it doesn't currently support block compression like `sparkey` does.
You can `zstd` compress a bit table when storing+transferring to/from a storage service like S3, but `bit` stores key/value pairs uncompressed in its data file.
Second, index creation is 32x slower than Sparkey:

```
BenchmarkBitCreate-10                    	       1	6780261500 ns/op	 8551552 B/op	     119 allocs/op
BenchmarkSparkeyCreateUncompressed-10    	       5	 209874583 ns/op	 8022249 B/op	 1000033 allocs/op
BenchmarkCdbCreate-10                    	       7	 157244560 ns/op	73709139 B/op	 3004380 allocs/op
```

We build a minimal perfect hash based on [cespare/mph](https://github.com/cespare/mph), which helps us build a reasonable small index table with a constant 4 memory accesses per `Get`. 
This is more than the average case for e.g. [colinmarc/cdb](https://github.com/colinmarc/cdb), but in the worst case `cdb` has to read a number of entries if there are collisions (and in practice we seem to be much faster).

## On-disk errors

Filesystem and network bugs happen, and `bit` is designed to detect and return an error if data is corrupted.
For keys, we check for strict equality between what is stored on-disk and what was passed to `Get`.
For values, we store a 32-bit checksum of the value in the key/value entry in the data file.
Every call to `Get` unconditionally recomputes and validates the `value`'s checksum.
As seen in the above benchmark, the performance cost for this is negligible, and the value of detecting corruption and returning an error rather than silently using it feels immense.

## Details

A `*bit.Table` is a pair of on-disk files: a log of key/value pairs (and some metadata, like a checksum of the value), and an index for efficient random lookup in that log of key/value pairs.
In particular the index is a minimal perfect hash based on [cespare/mph](https://github.com/cespare/mph).

Currently, we have a restriction that keys must be under 256 bytes in length.
Values can be up to 16 MB in length.
These restrictions aren't fundamental: we would just need to allocate more metadata bookkeeping space per entry in the `datafile` (currently we pack both sizes into a uint32).
