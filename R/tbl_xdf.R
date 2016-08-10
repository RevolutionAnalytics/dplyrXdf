#' @exportClass tbl_xdf
tbl_xdf <- setClass("tbl_xdf", contains="RxXdfData", slots=c(hasTblFile="logical"), prototype=list(hasTblFile=FALSE))


#' Create a tbl from an Xdf file
#'
#' @param data A data source object, representing an Xdf file or another type of data file on disk. Only file data sources are supported (Xdf, SAS, SPSS, delimited text).
#' @param file The file to use as working storage. If \code{NULL}, a new file will be created in the R working directory when results need to be saved. Cannot be \code{NULL} for non-Xdf data sources.
#' @param ... Other arguments that will be passed to \code{rxDataStep}.
#' @param hasTblFile Logical, has the tbl's working file been created yet?
#'
#' @details
#' A tbl wraps a RevoScaleR data source, allowing you to carry out data manipulation operations without modifying the source itself. It does this by copying the data to a temporary file, normally at the first point where the data is accessed. Usually you can let dplyrXdf create the temporary file for you; this saves time by avoiding unnecessary copying. However, if you specify a filename as an explicit argument to \code{tbl}, it will create that file using \code{rxDataStep} to copy the data from the source Xdf.
#'
#' As a side-effect, any datasets created in a dplyrXdf pipeline will be lost when you close your R session. If you want to keep the pipeline results, make sure you copy them to a permanent location on disk.
#'
#' @return
#' The \code{tbl} function returns an object of S4 class \code{tbl_xdf}, which extends \code{RxXdfdata}.
#' @section Note:
#' Many RevoScaleR functions that work with Xdf files will accept either an \code{RxXdfData} object, a character string giving the location of the file, or a data frame. The functions listed here only accept an \code{RxXdfData} object, or a tbl object wrapping the same.
#' @seealso
#' \code{\link[dplyr]{tbl}}, \code{\link[RevoScaleR]{RxXdfData}}
#' @rdname tbl
#' @export
tbl.RxXdfData <- function(data, file=NULL, hasTblFile=FALSE, ...)
{
    data <- as(data, "tbl_xdf")
    if(!is.null(file))
    {
        data@hasTblFile <- TRUE
        rxDataStep(data, file, ...)
        data@file <- rxXdfFileName(file)
    }
    else data@hasTblFile <- hasTblFile
    data
}


#' @param stringsAsFactors for non-Xdf data sources, whether to import character variables as factors. Defaults to TRUE.
#'
#' @details
#' By default, running \code{tbl} on a non-Xdf data source will convert any character variables to factors. This is the opposite of the usual behaviour for RevoScaleR functions that read non-Xdf data, and is for performance reasons: using factors instead of character variables can speed up functions like \code{\link[RevoScaleR]{rxCube}} and \code{\link[RevoScaleR]{rxSummary}} by significant margins.
#' @rdname tbl
#' @method tbl RxFileData
#' @export
tbl.RxFileData <- function(data, file=newTbl(data), hasTblFile=TRUE, stringsAsFactors=TRUE, ...)
{
    stopifnot(!is.null(file) && (is.character(file) || inherits(file, "RxXdfData")))
    data <- rxImport(data, file, stringsAsFactors=stringsAsFactors, ...)
    tbl(data, file=NULL, hasTblFile=hasTblFile) 
}


#' @export
tbl.grouped_tbl_xdf <- function(data, file=NULL, hasTblFile=FALSE, ...)
{
    if(!is.null(file))
    {
        data@hasTblFile <- TRUE
        rxDataStep(data, file, ...)
        data@file <- rxXdfFileName(file)
    }
    else data@hasTblFile <- hasTblFile
    data
}


#' @param x A file data source, or a tbl wrapping the same.
#' @return
#' The \code{tblFile} function returns the location where the intermediate and final results of a dplyr pipeline will be stored. If the tbl has not had any results written to it yet, or if this function is called on a non-tbl data source, a random filename is returned. The \code{tblSource} function is similar but returns a full data source, encapsulating file system information.
#'
#' The \code{tblFile} is deprecated; \code{tblSource} is preferred as it retains information about the file system and other parameters of the original data source.
#' @rdname tbl
#' @export
tblFile <- function(x)
{
    .Deprecated("tblSource", old="tblFile")
    if(hasTblFile(x)) x@file else newTbl(x)@file
}


#' @rdname tbl
#' @export
tblSource <- function(x)
{
    if(hasTblFile(x))
        as(x, "RxXdfData")  # do coerce to remove any grouping info
    else newTbl(x)
}


#' @return
#' The \code{hasTblFile} function returns TRUE if a tbl has a temporary file assigned to it (which also implies that it contains results from previous dplyr pipeline steps). It returns FALSE if no temporary file has yet been assigned, or if it is called on a non-tbl data source.
#' @rdname tbl
#' @export
hasTblFile <- function(x)
{
    if(!inherits(x, "tbl_xdf"))
        FALSE
    else x@hasTblFile
}


#' Get the variable names for a data source or tbl
#'
#' @param x A data source object, or tbl wrapping the same.
#' @details
#' This is a simple wrapper around the \code{names} method for classes inheriting from RxFileData.
#'
#' @seealso
#' \code{\link{RxXdfData}}, \code{\link{RxTextData}}, \code{\link{RxSasData}}, \code{\link{RxSpssData}}
#' @aliases tbl_vars
#' @rdname tbl_vars
#' @export
tbl_vars.RxFileData <- function(x)
{
    names(x)
}


#' Convert a data source or tbl to a data frame
#'
#' @param x A data source object, or tbl wrapping the same.
#' @param maxRowsByCols the maximum dataset size to convert, expressed in terms of rows times columns. Defaults to NULL, meaning no maximum.
#' @param ... Other arguments to \code{rxDataStep}.
#' @details
#' This is a simple wrapper around \code{\link[RevoScaleR]{rxDataStep}}, with the check on the maximum table size turned off. You should ensure that you have enough memory for your data.
#' @rdname as.data.frame
#' @export
as.data.frame.RxXdfData <- function(x, maxRowsByCols=NULL, ...)
{
    rxDataStep(x, outFile=NULL, maxRowsByCols=maxRowsByCols, ...)
}


#' @rdname as.data.frame
#' @export
as.data.frame.RxFileData <- function(x, maxRowsByCols=NULL, ...)
{
    rxDataStep(x, outFile=NULL, maxRowsByCols=maxRowsByCols, ...)
}


#' Delete data files for xdf tbls
#'
#' @param path Directory where the files are located. Defaults to the temporary directory where dplyrXdf creates tbl files (the R temporary directory for the native filesystem, or the dplyrXdf-created directory on HDFS).
#' @details
#' This is a utility function to delete the files generated by dplyrXdf. Note that all xdf files in the specified location will be removed!
#' @export
deleteXdfTbls <- function(path, fileSystem=rxGetFileSystem())
{
    if(inherits(fileSystem, "RxNativeFileSystem"))
    {
        if(missing(path))
            path <- tempdir()
        files <- dir(path, pattern="\\.xdf$", full.names=TRUE, ignore.case=.Platform$OS.type == "windows")
        # use unlink to allow for the possibility of composite xdfs
        unlink(files, recursive=TRUE)
    }
    else if(inherits(fileSystem, "RxHdfsFileSystem"))
    {
        if(missing(path))
            path <- .dxOptions$hdfsTempDir
        # rxHadoopFileExists doesn't always exist, duplicate its functionality
        pathExists <- (path == .dxOptions$hdfsTempDir && .dxOptions$hdfsTempDirCreated) ||
            (rxHadoopCommand(paste0("fs -test -e '", path, "'"), intern=FALSE) == 0)
        if(pathExists)
        {
            files <- rxHadoopListFiles(path, intern=TRUE)
            files <- substr(files, regexpr("\\s[[:alnum:][:punct:]]*$", files) + 1, nchar(files))
            files <- grep("xdfTBl", files, value=TRUE)
            lapply(files, rxHadoopRemoveDir, skipTrash=TRUE, intern=TRUE)
        }
    }
    invisible(NULL)
}

