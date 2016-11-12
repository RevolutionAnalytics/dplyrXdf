#' Pass RevoScaleR function arguments
#'
#' The RevoScaleR functions typically have several arguments beyond those used by dplyrXdf verbs. While usually you don't need to touch these, it can sometimes be useful to do so. For example, when using \code{mutate} or \code{transmute}, you could specify more complicated transformations via a \code{\link[=rxTransform]{transformFunc}}. Similarly, rather than chaining together a \code{mutate} and a \code{summarise} --- which would involve creating an intermediate file --- you could incorporate the variable transformation into the \code{summarise} itself.
#
#' Most of the one-table dplyrXdf verbs accept an \code{.rxArgs} argument as a way of transmitting these extra arguments to the underlying RevoScaleR code. This should be a named list specifying the names and values of the arguments to be passed. The exact arguments will vary depending on the verb in question; here is a list of the verbs and the underlying RevoScaleR function that they call:
#' \itemize{
#' \item \code{filter} and \code{select}: \code{rxDataStep}
#' \item \code{mutate} and \code{transmute}: \code{rxDataStep}
#' \item \code{summarise}: depending on the method chosen, \code{rxCube} or \code{rxSummary}
#' \item \code{arrange}: \code{rxSort}
#' \item \code{distinct}: \code{rxDataStep}
#' \item \code{factorise}: \code{rxFactors}
#' \item \code{doXdf}: \code{rxDataStep}
#' }
#'
#' You should use the \code{.rxArgs} argument with caution, as some verbs may modify the data as part of their normal functioning, so the results you get back may not be as expected. It's also easy to write convoluted code that makes your dplyrXdf pipelines harder to read. However, when working with big datasets this feature can help save a lot of processing time by avoiding unnecessary disk traffic.
#'
#' The following one-table verbs don't support the \code{.rxArgs} argument:
#' \itemize{
#'  \item \code{group_by}: this verb doesn't do any processing; it only sets things up for subsequent verbs.
#'  \item \code{do}: the underlying functionality is provided by data frames and \code{dplyr::do}.
#'  \item \code{rename}: this verb only changes the metadata in an xdf file; it doesn't touch the data itself.
#' }
#' @seealso
#' \code{\link{rxTransform}}
#' @aliases rxArgs .rxArgs
#' @name rxArgs
NULL

rxArgs <- function(dots, fromDo=FALSE)
{
    env <- if(length(dots) > 0) dots[[1]]$env else globalenv()
    exprs <- lapply(dots, "[[", "expr")

    if(!is.null(exprs$.rxArgs))
    {
        # some arguments have to be passed as unevaluated expressions: transforms and rowSelection
        rxArgs <- mapply(function(value, name) {
            if(name %in% c("transforms", "rowSelection"))
                value
            else eval(value, env)
        }, exprs$.rxArgs[-1], names(exprs$.rxArgs[-1]), SIMPLIFY=FALSE)
        exprs[[".rxArgs"]] <- NULL
    }
    else rxArgs <- NULL

    # capture output format
    # NULL -> data frame output
    # char -> target filename, xdf output
    # missing -> xdf tbl output, coded as NA in returned value
    if(".output" %in% names(exprs))
    {
        output <- exprs$.output
        exprs$.output <- NULL
        # turn off row x col size check if outputting to dataframe AND not otherwise specified AND not called from do()
        if(is.null(output) && !("maxRowsByCols" %in% names(rxArgs)) & !fromDo)
            rxArgs["maxRowsByCols"] <- list(NULL)
    }
    else output <- NA

    list(rxArgs=rxArgs, exprs=exprs, output=output, env=env)
}





