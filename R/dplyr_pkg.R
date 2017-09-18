#' dplyr backend for Xdf files
#'
#' The dplyrXdf package is a suite of tools to facilitate working with Microsoft R Server (MRS). Its features include:
#' \itemize{
#'   \item A backend to the popular \href{https://cran.r-project.org/web/packages/dplyr/index.html}{dplyr package} for the Xdf file format. Xdf files are a technology provided by MRS to break R's memory barrier: instead of keeping data in-memory in data frames, it is saved on disk. The data is then processed in chunks, so that you only need enough memory to handle each chunk.
#'   \item Interfaces to Microsoft SQL Server and HDInsight Hadoop and Spark clusters. dplyrXdf, in conjunction with dplyr, provides the ability to execute pipelines natively in-database and in-cluster, which for large datasets can be much more efficient than executing them locally.
#'   \item Several functions to ease working with Xdf files, including functions for file management and for transferring data to and from remote backends.
#'   \item Workarounds for various glitches and unexpected behaviour in MRS.
#' }
#' For more information, see the vignettes distributed with the package.
#' @docType package
#' @aliases dplyrXdf_package
#' @name dplyrXdf
NULL
