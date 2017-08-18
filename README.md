# dplyrXdf

The [dplyr package](https://cran.r-project.org/package=dplyr) is a toolkit for data transformation and manipulation. Since its introduction, dplyr has become very popular in the R community, for the way in which it streamlines and simplifies many common data manipulation tasks.

The dplyrXdf package implements a dplyr backend for [Microsoft R Server](https://www.microsoft.com/en-au/cloud-platform/r-server) (MRS). A key feature of MRS is that it allows you to break R's memory barrier. Instead of storing data in memory as data frames, it is stored on disk, in a file format identifiable by the `.xdf` extension. The data is then processed in chunks, so that you only need enough memory to store each chunk. This allows you to work with datasets of potentially unlimited size.

MRS includes a suite of data transformation and modelling functions in the RevoScaleR package that can handle xdf files. These functions are highly optimised and efficient, but their user interface can be complex. dplyrXdf allows you to work with xdf files within the framework supplied by dplyr, which reduces the learning curve and allows you to become productive more quickly. It works with data in the native filesystem and in HDFS, and can take advantage of a Spark or Hadoop cluster.

_Note that dplyrXdf is a shell on top of the existing functions provided by Microsoft R Server, which is a commercial distribution of R. You must have MRS installed to make use of dplyrXdf. In particular, Microsoft R Open does not include support for xdf files._

## Obtaining dplyrXdf

The current version of dplyrXdf is **0.10.0 beta**. You can download and install dplyrXdf from within R via the devtools package:

```r
install.packages("devtools")
devtools::install_github("RevolutionAnalytics/dplyrXdf")
```

dplyrXdf 0.10 requires dplyr 0.7 and Microsoft R Server release 8.0 or higher. If you are on an earlier release of MRS and/or dplyr, you can install dplyrXdf 0.9.2 instead: `install_github("RevolutionAnalytics/dplyrXdf@v0.9.2")`.

## Obtaining dplyr

At the moment, dplyr 0.7 is not in the MRAN snapshot that is the default repo for MRS users. You can install it from CRAN instead:

```r
install.packages("dplyr", repos="https://cloud.r-project.org")
```

