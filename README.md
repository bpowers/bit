bit: big immutable table
========================

`bit` implements a fast immutable hashtable for a set of static key/value pairs that is robust to on-disk errors.
It is especially useful if that set of key value pairs is too big to fit in main memory.

Accessing a `*bit.Table` requires no locks or atomic operations, and is allocation free.
At `GOMAXPROCS=8` it is within a factor of two the speed of a Go `map[string]string` and 4x faster than `go-sparkey`:

```
go test -bench=. -cpu 1,2,4,8
goos: darwin
goarch: arm64
pkg: github.com/bpowers/bit-benchmark
BenchmarkHashmap           	 9074078	       132.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkHashmap-2         	16998872	        70.26 ns/op	       0 B/op	       0 allocs/op
BenchmarkHashmap-4         	27842953	        43.79 ns/op	       0 B/op	       0 allocs/op
BenchmarkHashmap-8         	52205973	        22.61 ns/op	       0 B/op	       0 allocs/op
BenchmarkBit               	 4061503	       289.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkBit-2             	 8022603	       149.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkBit-4             	14951570	        80.07 ns/op	       0 B/op	       0 allocs/op
BenchmarkBit-8             	30102850	        39.35 ns/op	       0 B/op	       0 allocs/op
BenchmarkSparkey           	 1497722	       792.9 ns/op	     592 B/op	      11 allocs/op
BenchmarkSparkey-2         	 2669196	       430.8 ns/op	     592 B/op	      11 allocs/op
BenchmarkSparkey-4         	 5000991	       238.7 ns/op	     592 B/op	      11 allocs/op
BenchmarkSparkey-8         	 8294257	       163.8 ns/op	     592 B/op	      11 allocs/op
BenchmarkCdb               	  689727	      1653 ns/op	     116 B/op	       3 allocs/op
BenchmarkCdb-2             	  698636	      1664 ns/op	     116 B/op	       3 allocs/op
BenchmarkCdb-4             	  695551	      1903 ns/op	     116 B/op	       3 allocs/op
BenchmarkCdb-8             	  372199	      3258 ns/op	     116 B/op	       3 allocs/op
```

In particular, `bit` makes two large tradeoffs to achieve high `Get` performance.
First, it doesn't currently support block compression like `sparkey` does.
You can `zstd` compress a bit table when storing+transferring to/from a storage service like S3, but `bit` stores key/value pairs uncompressed in its data file.
Second, index creation is 32x slower than Sparkey.
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
