#dplyrXdf

The [dplyr package](https://cran.r-project.org/web/packages/dplyr/index.html) is a popular toolkit for data transformation and manipulation. Over the last year and a half, dplyr has become a hot topic in the R community, for the way in which it streamlines and simplifies many common data manipulation tasks.

Out of the box, dplyr supports data frames, data tables (from the data.table package), and the following SQL databases: MySQL/MariaDB, SQLite, and PostgreSQL. However, a feature of dplyr is that it's _extensible_: by writing a specific backend, you can make it work with many other kinds of data sources. For example the development version of the [RSQLServer package](https://github.com/imanuelcostigan/RSQLServer) implements a dplyr backend for Microsoft SQL Server.

The dplyrXdf package implements such a backend for the xdf file format, a technology supplied as part of Revolution R Enterprise. All of the data transformation and modelling functions provided with Revolution R Enterprise support xdf files, which allow you to break R’s memory barrier: by storing the data on disk, rather than in memory, they make it possible to work with multi-gigabyte or terabyte-sized datasets.

dplyrXdf brings the benefits of dplyr to xdf files, including support for pipeline notation, all major verbs, and the ability to incorporate xdfs into dplyr pipelines. It also provides some additional benefits which are more specific to working with xdfs:

The RevoScaleR functions require keeping track of where your data is saved. In some situations, writing a function’s output to the same file as its input is allowed, while in others, it causes problems. You can often end up with many different version of the data scattered around your filesystem, introducing reproducibility problems and making it difficult to keep track of changes. dplyrXdf abstracts this task of file management away, so that you can focus on the data itself.

Related to the above, the source xdf to a dplyrXdf pipeline is never modified. This provides a measure of security, so that even if there are bugs in your code (maybe you meant to use a `mutate` rather than a `transmute`), the original data is safe.

Consistency of interface: functions like rxCube and rxSummary use formulas in different ways, because they are designed to do slightly different things. Similarly, many RevoScaleR functions use factors but don’t automatically create those factors; or they require handholding when trying to combine factor with non-factor data. With dplyrXdf, you don’t have to remember which formula syntax goes with which function, or create factors yourself. If you do have to create factors, it provides a new verb (`factorise`) to streamline this as well.

The verbs in dplyrXdf all read from xdf files and write to xdf files. The data is thus never read entirely into memory, so a dplyrXdf pipeline will work with datasets that are arbitrarily large.

##Obtaining dplyrXdf

The package is available for download from Github, at [`https://github.com/Hong-Revo/dplyrXdf`](https://github.com/Hong-Revo/dplyrXdf). If you have the devtools package installed, you can download and install it from within R using the command `devtools::install_github("RevolutionAnalytics/dplyrXdf")`.

_Note that dplyrXdf is a shell on top of the existing functions provided by Revolution R Enterprise, which is the commercial (paid) distribution of R from Revolution Analytics. You must be an existing RRE customer to make use of dplyrXdf. It will not work with Revolution R Open, as RRO doesn't support xdf files._
