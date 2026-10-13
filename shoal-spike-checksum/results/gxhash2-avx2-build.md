# gxhash 2.3.1 with its avx2 feature, built with rustc 1.100.0-nightly (2026-09-04)

```
$ RUSTFLAGS="-C target-cpu=znver1" cargo build --release --features gxhash2-avx2
error[E0635]: unknown feature `stdsimd`
 --> ~/.cargo/registry/src/.../gxhash-2.3.1/src/lib.rs:2:68
  |
2 | #![cfg_attr(all(feature = "avx2", target_arch = "x86_64"), feature(stdsimd))]
  |                                                                    ^^^^^^^
error: could not compile `gxhash` (lib) due to 1 previous error
```
