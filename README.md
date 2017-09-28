# dplyrXdf

The dplyrXdf package is a suite of tools to facilitate working with [Microsoft Machine Learning Server](https://www.microsoft.com/en-au/cloud-platform/r-server), previously known as Microsoft R Server (MRS). Its features include:

- A backend to the popular [dplyr package](http://dplyr.tidyverse.org) for the Xdf file format. Xdf files are a technology provided by MRS to break R's memory barrier: instead of keeping data in-memory in data frames, it is saved on disk. The data is then processed in chunks, so that you only need enough memory to handle each chunk.
- Interfaces to Microsoft SQL Server and HDInsight Hadoop and Spark clusters. dplyrXdf, in conjunction with dplyr, provides the ability to execute pipelines natively in-database and in-cluster, which for large datasets can be much more efficient than executing them locally.
- Several functions to ease working with Xdf files, including functions for file management and for transferring data to and from remote backends.
- Workarounds for various glitches and unexpected behaviour in MRS and dplyr.


## Obtaining dplyrXdf

The current version of dplyrXdf is **1.0.0**. You can download and install dplyrXdf from within R via the devtools package:

```r
install.packages("devtools")
devtools::install_github("RevolutionAnalytics/dplyrXdf")
```

dplyrXdf requires Microsoft R Server release 8.0 or later, and dplyr 0.7 or later. If you want to use sparklyr and SQL Server integration, you will also have to install the dbplyr, sparklyr and odbc packages (and their dependencies).

If you are using MRS 9.1 or earlier, the necessary packages will not be in the MRAN snapshot that is your default repo. You can install them from CRAN instead:

```r
install.packages("dplyr", repos="https://cloud.r-project.org")
# optional
install.packages(c("dbplyr", "odbc"), repos="https://cloud.r-project.org")
install.packages(c("dbplyr", "sparklyr"), repos="https://cloud.r-project.org")
```

Make sure you install dplyr 0.7 _before_ you install dplyrXdf.

## Earlier versions

The previous version of dplyrXdf, 0.9.2, is also available. You can install this with `install_github("RevolutionAnalytics/dplyrXdf@v0.9.2")`. This version requires dplyr 0.5 or earlier; it may run into problems with dplyr 0.7.


