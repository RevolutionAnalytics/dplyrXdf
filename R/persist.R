#' Copy an xdf tbl to a permanent storage location.
#'
#' @param .data An xdf tbl
#' @param file Character string giving the name of the output xdf file
#' @param overwrite If the outfile already exists, should it be overwritten?
#' @param move Should the tbl file be moved or copied?
#' @param composite Create a composite Xdf or normal? The default is to create the same type of file as the input.
#' @param ... Other arguments to \code{\link[RevoScaleR]{rxDataStep}}
#'
#' @details
#' By default, the underlying data for an xdf tbl is saved as a file in the R temporary directory, and is managed by dplyrXdf. This can cause confusion and errors when a tbl is reused by later dplyrXdf operations, which can overwrite or delete the data.
#'
#' The \code{persist} verb is a simple routine that either copies or moves the data into a new Xdf file at the location given by \code{outFile}. This ensures that the data will not be modified by dplyrXdf, and will be persistent beyond the end of the R session.
#'
#' @return
#' An \code{RxXdfData} object pointing to the new xdf file.
#'
#' @seealso
#' \code{\link{as_xdf}}, \code{\link{as.data.frame.RxXdfData}}, \code{\link[RevoScaleR]{rxDataStep}}
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' tbl <- mutate(mtx, mpg2=2 * mpg)
#' persist(tbl, "mtcars_mutate.xdf", move=FALSE)
#' persist(tbl, "mtcars-composite", composite=TRUE)  # composite Xdf names cannot contain _ or .
#' @rdname persist
#' @export
persist <- function(.data, ...)
{
    UseMethod("persist")
}


#' @rdname persist
#' @export
persist.tbl_xdf <- function(.data, file, composite=is_composite_xdf(.data), move=TRUE, overwrite=TRUE, ...)
{
    out <- as_xdf(.data, file, composite=composite, overwrite=overwrite)
    if(move)
        delete_xdf(.data)
    out
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
