#' dplyr backend for Xdf files
#'
#' The dplyrXdf package implements a \link[dplyr]{dplyr} interface for Microsoft R Server (MRS). A key feature of MRS is that it allows you to break R's memory barrier. Instead of storing data in memory as data frames, it is stored on disk, in a file format identifiable by the .xdf extension. The data is then processed in chunks, so that you only need enough memory to store each chunk. This allows you to work with datasets of potentially unlimited size.
#'
#' @section Verbs:
#'
#' dplyrXdf supports all the basic dplyr single-table verbs, plus some additions:
#' \itemize{
#'   \item \code{subset}, \code{filter} and \code{select} to choose rows and columns
#'   \item \code{mutate} and \code{transmute} to do data transformation
#'   \item \code{group_by} to define groups
#'   \item \code{summarise}, \code{do} and \code{doXdf} to carry out computations on grouped data
#'   \item \code{arrange} to sort by variables
#'   \item \code{rename} to rename columns
#'   \item \code{distinct} to drop duplicates
#'   \item \code{factorise} to create factors
#'   \item \code{persist} to save a pipeline's output to a persistent location (see below).
#' }
#' Similarly, dplyrXdf also supports all the table-join verbs from dplyr, \emph{except} for the set operations \code{intersect}, \code{setdiff} and \code{setequal}:
#' \itemize{
#'   \item \code{left_join}, \code{right_join}, \code{inner_join} and \code{full_join}
#'   \item \code{anti_join} and \code{semi_join}
#'   \item \code{union} and \code{union_all}
#' }
#'
#' @section Utilities:
#'
#' dplyrXdf provides a suite of utility functions to ease working with data in Xdf format:
#' \itemize{
#'   \item \code{sample_n} and \code{sample_frac} for random sampling
#'   \item \code{cbind} and \code{rbind} methods for combining Xdf files
#'   \item \code{[[}, \code{$} and \code{pull} methods for extracting a column
#'   \item \code{as_xdf} methods to import data into an Xdf file
#'   \item \code{as.data.frame} to convert an Xdf file into a data frame
#'   \item \code{copy_xdf}, \code{move_xdf}, \code{rename_xdf} and \code{delete_xdf} to manage Xdf data
#'   \item \code{copy_to}, \code{collect} and \code{compute} methods for transferring datasets to and from HDFS
#'   \item \link[=hdfs_dir]{Several other functions} for working with HDFS files: upload; download; directory listing; copy, move and delete files; etc.
#' }
#'
#' @section Principles:
#'
#' A basic idea behind dplyrXdf is that all operations should be isolated from the original data source. The benefit of this is that it protects your data: if you accidentally delete a variable or make a wrong transformation, your original file remains intact. This principle is also consistent with dplyr pipelines and R data operations in general.
#'
#' Another benefit of dplyrXdf is that it handles file management duties for you. All the data files in a pipeline are created in a special working directory, and only the final output of a pipeline is retained, which keeps intermediate files from cluttering up your filesystem. When you quit R, all data files created are deleted. To persist a file beyond the end of a session, dplyrXdf provides the special \code{persist} verb, as well as a \code{.outFile} argument for all verbs to specify a location for saving the output.
#'
#' @section Non-Xdf and non-local data sources:
#' dplyrXdf handles non-Xdf (file) data sources by importing the data into a temporary Xdf file when it is first accessed. Data sources handled this way include delimited text (\code{\link{RxTextData}}), SAS (\code{\link{RxSasData}}) and SPSS (\code{\link{RxSpssData}}). For SQL database sources (\code{\link{RxOdbcData}} and \code{\link{RxTeradata}}), consider using the dplyr backend specific to your database, or if that is not available, importing the data as Xdf.
#'
#' HDFS data is supported. In general, working with datasets in HDFS should be much the same as with data in the native filesystem. dplyrXdf provides functions to quickly and easily upload and download files and datasets, as well as managing files stored in HDFS. Most dplyr verbs will also work with HDFS data, although there are a few exceptions; see the help pages for each verb for more information.
#'
#' @docType package
#' @aliases dplyrXdf_package
#' @name dplyrXdf
NULL
