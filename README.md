#dplyrXdf

The [dplyr package](https://cran.r-project.org/package=dplyr) is a toolkit for data transformation and manipulation. Since its introduction, dplyr has become very popular in the R community, for the way in which it streamlines and simplifies many common data manipulation tasks.

The dplyrXdf package implements a dplyr backend for [Microsoft R Server](https://www.microsoft.com/en-au/cloud-platform/r-server) (MRS). A key feature of MRS is that it allows you to break R's memory barrier. Instead of storing data in memory as data frames, it is stored on disk, in a file format identifiable by the `.xdf` extension. The data is then processed in chunks, so that you only need enough memory to store each chunk. This allows you to work with datasets of potentially unlimited size.

MRS includes a suite of data transformation and modelling functions in the RevoScaleR package that can handle xdf files. These functions are highly optimised and efficient, but their user interface can be complex. dplyrXdf allows you to work with xdf files within the framework supplied by dplyr, which reduces the learning curve and allows you to become productive more quickly.

##Obtaining dplyrXdf

You can get dplyrXdf from GitHub. If you have the devtools package, you can download and install it from within R using the command `devtools::install_github("RevolutionAnalytics/dplyrXdf")`.

dplyrXdf is also installed on the current version of the [Windows Data Science VM](https://azure.microsoft.com/en-us/marketplace/partners/microsoft-ads/standard-data-science-vm/) on Azure. 

_Note that dplyrXdf is a shell on top of the existing functions provided by Microsoft R Server, which is a commercial distribution of R. You must have MRS installed to make use of dplyrXdf. In particular, Microsoft R Open does not include support for xdf files._
