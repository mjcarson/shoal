# The catalog's described objects

`Images.yaml` describes the `Images` bucket's objects rather than holding them: how many, how
their sizes are drawn, a seed, and the generator their bytes come from (`splitmix64-ctr/1`). A
driver makes any object, and any range of it, from its index alone, and reads every
acknowledged object back against its bytes made again. The folder holds no table file, so a run
over it runs object arms alone.
