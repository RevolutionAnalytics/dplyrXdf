#' Get or set dplyrXdf options
#'
#' @param ... Options to define, using \code{name = value}. Valid options are listed under "Details" below.
#'
#' @details
#' Use this function to get or set various options that control dplyrXdf's functionality.
#' \itemize{
#'  \item \code{useExecBy}: whether to do by-group processing with \code{\link[RevoScaleR]{rxExecBy}}. Defaults to TRUE if Microsoft R Server 9.1 or later is installed. Requires MRS 9.1.
#'  \item \code{localWorkDir}: working directory to store xdf tbl files. It is recommended to use \code{\link{get_dplyrxdf_dir}} and \code{\link{set_dplyrxdf_dir}} for getting and setting this option.
#'  \item \code{hdfsWorkDir}: working directory to store xdf tbl files in HDFS. Not currently used; for future expansion.
#'  \item \code{rowsPerRead}: default number of rows per chunk when writing xdf files.
#'  \item \code{dplyrVersion}: version of dplyr installed.
#' }
#' 
#' @return
#' Returns a list of the option values before any changes are carried out.
#' @seealso
#' \code{\link{options}}
#' @export
dplyrxdf_options <- function(...)
{
    oldOpts <- as.list(.dxOptions)
    if(nargs() == 0)
        return(oldOpts)
    dots <- list(...)
    nams <- names(dots)
    opts <- names(.dxOptions)

    if(!all(nams %in% opts))
    {
        badOpts <- nams[!(nams %in% opts)]
        stop("invalid dplyrXdf option(s): ", paste(badOpts, collapse=", "))
    }

    readOnlyOpts <- c("dplyrVersion", "hdfsWorkDirCreated")
    if(any(readOnlyOpts %in% nams))
        stop("attempt to set a read-only parameter")

    rxver <- packageVersion("RevoScaleR")
    if("useExecBy" %in% nams && (rxver < package_version("9.1")))
        stop("rxExecBy not available in this RevoScaleR version")

    for(o in nams)
    {
        if(o == "hdfsWorkDir")
        {
            set_dplyrxdf_dir(dots[[o]], "hdfs")
            .dxOptions$hdfsWorkDirCreated <- FALSE
        }
        else if(o == "nativeWorkDir")
            set_dplyrxdf_dir(dots[[o]], "native")
        else .dxOptions[[o]] <- dots[[o]]
    }
    invisible(oldOpts)
}
