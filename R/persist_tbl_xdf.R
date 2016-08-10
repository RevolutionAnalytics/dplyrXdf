#' Copy an xdf tbl to a permanent storage location.
#'
#' @param data An xdf tbl
#' @param outFile Character string giving the name of the output xdf file
#' @param overwrite If the outfile already exists, should it be overwritten?
#' @param deleteOldTbl Should the original tbl file be removed?
#' @param ... Other arguments to \code{\link[RevoScaleR]{rxDataStep}}
#'
#' @details
#' By default, the underlying data for an xdf tbl is saved as a file in the R temporary directory, and is managed by dplyrXdf. This can cause confusion when a tbl is reused by later dplyrXdf operations, which can overwrite or delete the data.
#'
#' The \code{persist} verb is a simple shell around \code{rxDataStep} that copies the data into a new xdf file at the location given by \code{outFile}. This ensures that the data will not be modified by dplyrXdf, and will be persistent beyond the end of the R session.
#'
#' @return
#' An \code{RxXdfData} object pointing to the new xdf file.
#'
#' @seealso
#' \code{\link{as.data.frame.RxXdfData}}, \code{\link[RevoScaleR]{rxDataStep}}
#' @rdname persist
#' @export
persist <- function(data, ...)
{
    UseMethod("persist")
}


#' @rdname persist
#' @export
persist.tbl_xdf <- function(data, outFile, overwrite=TRUE, deleteOldTbl=FALSE, ...)
{
    if(deleteOldTbl)
        on.exit(deleteTbl(data))
    rxDataStep(data, outFile, overwrite=overwrite, rowsPerRead=.dxOptions$rowsPerRead, ...)
}


#' @details
#' Calling this verb on a non-tbl data source (eg a raw \code{RxXdfData} object) will give a warning and return the data source unchanged.
#'
#' @rdname persist
#' @export
persist.RxFileData <- function(data, ...)
{
    warning("dataset is already persistent")
    data
}
