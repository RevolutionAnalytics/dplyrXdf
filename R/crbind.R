#' Combine data sources by rows or columns
#'
#' @param ... Objects to combine. Can be \code{RxXdfData} or \code{tbl_xdf} data sources.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param deparse.level For compatibility with the base \code{cbind} and \code{rbind} generics. Not used.
#'
#' @details
#' Due to the specifics of how R searches for methods for \code{cbind} and \code{rbind}, there are a few caveats to using these. First, \emph{all} the objects to be combined must be Xdf files for the correct method to be found; in particular, trying to combine a data frame and an Xdf file will result in an error (or bad output). Second, specifying the \code{rxArgs} argument will fail if called from the generic. These issues can be worked around by calling the Xdf method directly: \code{cbind.RxXdfData(xdf1, xdf2, .rxArgs=...)}, or \code{rbind.RxXdfData(xdf1, xdf2, .rxArgs=...)}.
#'
#' These methods also have some limitations compared to \code{cbind} and \code{rbind} for data frames. The \code{cbind.RxXdfData} method will drop columns that have duplicate names, with a warning; this behaviour is different to \code{cbind} with data frames, which will create an output df with duplicated names; and to \code{dplyr::bind_cols}, which will rename columns to be unique. The \code{rbind.RxXdfData} method will give an error if the columns in all the objects don't match, rather than creating new columns.
#'
#' These methods support HDFS data in the local compute compute context, but not the Hadoop or Spark compute contexts.
#'
#' @seealso
#' \code{\link[base]{cbind}} and \code{\link[base]{rbind}} in base R,
#' \code{\link[dplyr]{bind_cols}} and \code{\link[dplyr]{bind_rows}} in package dplyr
#'
#' @examples
#' # cbind two Xdf files together
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' tbl <- transmute(mtx, mpg2 = 2 * mpg)
#' cbind(mtx, tbl)
#'
#' @aliases cbind rbind
#' @rdname crbind
#' @rawNamespace export(cbind.RxXdfData)
#' @method cbind RxXdfData
#' @export
cbind.RxXdfData <- function(..., deparse.level=1, .outFile=tbl_xdf(lst[[1]]), .rxArgs)
{
    stopIfDistribCC("cbind for Rx source objects not supported in Hadoop/Spark compute context")
    if(!missing(deparse.level))
        warn("cbind for Rx source objects doesn't use deparse.level")

    lst <- list(...)
    if(length(lst) < 1)
        return(NULL)

    # wart: specifying .rxArgs will fail if called from the generic, because of how cbind/rbind dispatch
    arglst <- doExtraArgs(list(), lst[[1]], rlang::enexpr(.rxArgs), .outFile)
    
    # for data frame output, create an xdf and convert
    dfOutput <- is.null(arglst$outFile)
    if(dfOutput)
        arglst$outFile <- tbl_xdf(lst[[1]])

    # cannot set rowsPerRead with append="cols"
    arglst$rowsPerRead <- NULL

    dupNameWarn <- FALSE
    for(i in seq_along(lst))
    {
        if(i == 1)
            arglst$append <- "none"
        else
        {
            arglst$append <- "cols"
            # warn if duplicate colnames found
            if(!dupNameWarn)
                dupNameWarn <- any(names(lst[[i]]) %in% names(output))
        }
        arglst$inData <- lst[[i]]
        output <- callRx("rxDataStep", arglst)
    }

    if(dupNameWarn)
        warning("duplicate column names found; output dataset may be incomplete", call.=FALSE)
    if(dfOutput)
    {
        on.exit(deleteIfTbl(output))
        as.data.frame(output)
    }
    else output
}


#' @examples
#' # rbind two Xdf files together
#' mtx2 <- as_xdf(mtcars)
#' rbind(mtx, mtx2)
#'
#' # combine an Xdf file and a data frame: must explicitly call RxXdfData method
#' rbind.RxXdfData(mtx, mtcars)
#'
#' # save to a persistent Xdf file: again, must explicitly call RxXdfData method
#' cbind.RxXdfData(mtx, tbl, .outFile="mtcars_cbind.xdf")
#' @rdname crbind
#' @rawNamespace export(rbind.RxXdfData)
#' @method rbind RxXdfData
#' @export
rbind.RxXdfData <- function(..., deparse.level=1, .outFile=tbl_xdf(lst[[1]]), .rxArgs)
{
    stopIfDistribCC("rbind for Rx source objects not supported in Hadoop/Spark compute context")
    if(!missing(deparse.level))
        warn("rbind for Rx source objects doesn't use deparse.level")

    lst <- list(...)
    if(length(lst) < 1)
        return(NULL)

    # wart: specifying .rxArgs will fail if called from the generic, because of how cbind/rbind dispatch
    arglst <- doExtraArgs(list(), lst[[1]], rlang::enexpr(.rxArgs), .outFile)

    # for data frame output, create an xdf and convert
    dfOutput <- is.null(arglst$outFile)
    if(dfOutput)
        arglst$outFile <- tbl_xdf(lst[[1]])

    for(i in seq_along(lst))
    {
        arglst$append <- if(i == 1) "none" else "rows"
        arglst$inData <- lst[[i]]
        output <- callRx("rxDataStep", arglst)
    }

    if(dfOutput)
    {
        on.exit(deleteIfTbl(output))
        as.data.frame(output)
    }
    else output
}


