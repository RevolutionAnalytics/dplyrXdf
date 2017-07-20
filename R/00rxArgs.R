#' Pass RevoScaleR function arguments
#'
#' The RevoScaleR functions typically have several arguments beyond those used by dplyrXdf verbs. While usually you don't need to touch these, it can sometimes be useful to do so. For example, when using \code{mutate} or \code{transmute}, you could specify more complicated transformations via a \code{\link[=rxTransform]{transformFunc}}. Similarly, rather than chaining together a \code{mutate} and a \code{summarise} --- which would involve creating an intermediate file --- you could incorporate the variable transformation into the \code{summarise} itself.
#
#' Most of the one-table dplyrXdf verbs accept an \code{.rxArgs} argument as a way of transmitting these extra arguments to the underlying RevoScaleR code. This should be a named list specifying the names and values of the arguments to be passed. The exact arguments will vary depending on the verb in question; here is a list of the verbs and the underlying RevoScaleR function that they call:
#' \itemize{
#'  \item \code{subset}, \code{filter} and \code{select}: \code{rxDataStep}
#'  \item \code{mutate} and \code{transmute}: \code{rxDataStep}
#'  \item \code{summarise}: depending on the method chosen, \code{rxCube} or \code{rxSummary}
#'  \item \code{arrange}: \code{rxSort}
#'  \item \code{rename}: \code{rxDataStep} (only if data movement is required)
#'  \item \code{distinct}: \code{rxDataStep}
#'  \item \code{factorise}: \code{rxFactors}
#'  \item \code{doXdf}: \code{rxDataStep}
#'  \item \code{persist}: \code{rxDataStep}
#' }
#'
#' You should use the \code{.rxArgs} argument with caution, as some verbs may modify the data as part of their normal functioning, so the results you get back may not be as expected. It's also easy to write convoluted code that makes your dplyrXdf pipelines harder to read. However, when working with big datasets this feature can help save a lot of processing time by avoiding unnecessary disk traffic.
#'
#' The following one-table verbs don't support the \code{.rxArgs} argument:
#' \itemize{
#'  \item \code{group_by}: this verb doesn't do any processing; it only sets things up for subsequent verbs.
#'  \item \code{do}: the underlying functionality is provided by data frames and \code{dplyr::do}.
#' }
#' @seealso
#' \code{\link{rxTransform}}
#' @aliases rxArgs .rxArgs
#' @name rxArgs
NULL

doExtraArgs <- function(arglst, .data, .rxArgs, .outFile)
{
    arglst <- rlang::modify(arglst, overwrite=TRUE, rowsPerRead=.dxOptions$rowsPerRead)
    if(rlang::is_lang(.rxArgs))
        arglst <- rlang::modify(arglst, rlang::splice(rlang::lang_args(.rxArgs)))

    if(!inherits(.outFile, "tbl_xdf"))
    {
        composite <- isCompositeXdf(.data)
        if(is.null(.outFile))
            arglst["maxRowsByCols"] <- list(NULL)
        else if(inherits(.outFile, "RxXdfData"))
            arglst$outFile <- .outFile
        else if(is.character(.outFile))
        {
            .outFile <- validateXdfFile(.outFile, composite)
            arglst$outFile <- RxXdfData(.outFile, fileSystem=rxGetFileSystem(.data), createCompositeSet=composite)
        }
        else
        {
            warning("unexpected value for .outFile ignored")
            arglst$outFile <- tbl_xdf(.data)
        }
    }
    else arglst$outFile <- .outFile

    arglst
}

