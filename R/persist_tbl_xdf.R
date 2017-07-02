#' Copy an xdf tbl to a permanent storage location.
#'
#' @param .data An xdf tbl
#' @param outFile Character string giving the name of the output xdf file
#' @param overwrite If the outfile already exists, should it be overwritten?
#' @param move Should the tbl file be moved or copied?
#' @param composite Create a composite Xdf or normal? The default is to create the same type of file as the input.
#' @param ... Other arguments to \code{\link[RevoScaleR]{rxDataStep}}
#'
#' @details
#' By default, the underlying data for an xdf tbl is saved as a file in the R temporary directory, and is managed by dplyrXdf. This can cause confusion and errors when a tbl is reused by later dplyrXdf operations, which can overwrite or delete the data.
#'
#' The \code{persist} verb is a simple routine that either copies or moves the data into a new xdf file at the location given by \code{outFile}. This ensures that the data will not be modified by dplyrXdf, and will be persistent beyond the end of the R session.
#'
#' @return
#' An \code{RxXdfData} object pointing to the new xdf file.
#'
#' @seealso
#' \code{\link{as.data.frame.RxXdfData}}, \code{\link[RevoScaleR]{rxDataStep}}
#' @rdname persist
#' @export
persist <- function(.data, ...)
{
    UseMethod("persist")
}


#' @rdname persist
#' @export
persist.tbl_xdf <- function(.data, outFile, overwrite=TRUE, move=FALSE, composite=NULL, ...)
{
    if(is.null(composite))
        composite <- isCompositeXdf(.data)
    # use OS move/copy command if on the local filesystem
    if(inherits(rxGetFileSystem(.data), "RxNativeFileSystem"))
    {
        compositeIn <- isCompositeXdf(.data)
        inFile <- validateXdfFile(.data@file, compositeIn)
        outFile <- validateXdfFile(outFile, composite)

        if(compositeIn == composite)
        {
            if(move)
                file.rename(inFile, outFile)
            else file.copy(inFile, outFile, overwrite=overwrite, recursive=TRUE)
        }
        else
        {
            # save as desired type
            out <- RxXdfData(outFile, createCompositeSet=composite)
            rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead)
            if(move)
                unlink(inFile, recursive=TRUE)
        }
        RxXdfData(outFile, createCompositeSet=composite)
    }
    else  # do it the long way otherwise
    {
        if(move)
            on.exit(deleteIfTbl(.data))
        rxDataStep(.data, outFile, overwrite=overwrite, ...)
    }
}


#' @details
#' Calling this verb on a non-tbl data source (eg a raw \code{RxXdfData} object) will give a warning and return the data source unchanged.
#'
#' @rdname persist
#' @export
persist.RxFileData <- function(.data, ...)
{
    warning("dataset is already persistent")
    .data
}
