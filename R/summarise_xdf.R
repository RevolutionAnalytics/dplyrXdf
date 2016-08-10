#' Summarise multiple values to a single value
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Name-value pairs of summary functions like \code{\link{min}()}, \code{\link{mean}()}, \code{\link{max}()} etc.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details. See "Details" below for how to change the summarisation method.
#'
#' @details
#' There are 5 possible methods for doing the summarisation. To choose which method is used, specify a \code{.method} argument in the call to \code{summarise}, with a number from 1 to 5.
#' \enumerate{
#'  \item use \code{\link[RevoScaleR]{rxCube}}, cbind data frames together: only \code{n()}, \code{mean()}, \code{sum()} supported, grouped data only (fast)
#'  \item use \code{\link[RevoScaleR]{rxSummary}}, cbind data frames together: stats in rxSummary supported (fast)
#'  \item as 2), but build classification levels by pasting the grouping variable(s) together (moderately fast)
#'  \item split into multiple Xdfs by group, run \code{dplyr::summarise} on each, rbind xdfs together: arbitrary stats supported (slow)
#'  \item split into multiple Xdfs by group, run \code{rxSummary} on each, rbind xdfs together: stats in rxSummary supported (slowest, most scalable)
#' }
#' The default method is 1 if the data is grouped and the requested summary statistics are supported by \code{rxCube}; otherwise 2 if the requested statistics are supported by \code{rxSummary}; otherwise 4. Method 3 is supplied for the case where the product of factor levels for the grouping variables exceeds 2^32 - 1, a known limitation of \code{rxCube} and \code{rxSummary}.
#'
#' Supplying custom functions to summarise is supported, but they must be \emph{named} functions (and will automatically cause \code{.method=4} to be selected). Anonymous functions will cause an error.
#'
#' @seealso
#' \code{\link[dplyr]{summarise}} in package dplyr, \code{\link[RevoScaleR]{rxCube}}, \code{\link[RevoScaleR]{rxSummary}}
#' @aliases summarise summarize
#' @rdname summarise
#' @export
summarise_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    if(!is.null(dots$.method))
    {
        .method <- dots$.method$expr
        dots[[".method"]] <- NULL
    }
    else .method <- NULL

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs
    attr(exprs, "env") <- dots$env

    stats <- sapply(exprs, function(term) as.character(term[[1]]))
    needs_mutate <- vapply(exprs, function(e) {
        (length(e) < 2 || !is.name(e[[2]])) && !identical(e, quote(n()))
    }, logical(1))
    if(any(needs_mutate))
        stop("summarise with xdf tbls only works with named variables, not expressions")  # TODO: do actual mutate

    grps <- groups(.data)

    # options:
    # 1- one or multiple calls to rxCube, cbind data frames together: only n, mean, sum supported, grouped data only (fast)
    # 2- one call to rxSummary, cbind data frames together: stats in rxSummary supported (fast)
    # 3- as 2, but build classification levels by pasting grouping vars together (work around cube size limitation in rxCube/rxSummary)
    # 4- split into multiple xdfs, dplyr::summarise on each, rbind xdfs together: arbitrary stats supported (slow)
    # 5- split into multiple xdfs, rxSummary on each, rbind xdfs together: stats in rxSummary supported (slowest, most scalable)
    .method <- select_smry_method(stats, .method, grps)

    # only summarise methods 1-3 work with HDFS
    if(.method > 3)
        stopIfHdfs(sprintf("chosen summarise method not (%d) supported on HDFS",
                           .method))

    smry_func <- switch(.method,
        smry_rxCube, smry_rxSummary, smry_rxSummary2, smry_rxSplit_dplyr, smry_rxSplit)
    smry <- smry_func(.data, grps, stats, exprs, rxArgs)

    .data <- if(is.data.frame(smry))
        tbl(rxDataStep(smry, tblSource(.data), overwrite=TRUE), hasTblFile=TRUE)
    else tbl(smry, hasTblFile=TRUE)

    # generate new grouping info if necessary
    numGroups <- length(grps)
    if(numGroups > 1)
        group_by_(.data, .dots=grps[-numGroups])
    else .data
}


select_smry_method <- function(stats, method, groups=NULL)
{
    cubeStats <- all(stats %in% c("mean", "sum", "n"))
    simpleStats <- all(stats %in% c("mean", "sum", "sd", "var", "n", "min", "max"))
    grouped <- !is.null(groups)
    defaultMethod <- is.null(method)
    if(!defaultMethod && !(method %in% 1:5))
        stop("unknown method selection for summarise (must be a number from 1 to 5)")
    if(defaultMethod)
    {
        if(cubeStats && grouped)
            method <- 1
        else if(simpleStats)
            method <- 2
        else method <- 4
    }
    else  # various sanity checks
    {
        if(method == 1 && !grouped)
        {
            warning("rxCube method only works for grouped data", call.=FALSE)
            method <- Recall(stats=stats, method=NULL, groups=groups)
        }
        else if(method == 1 && !cubeStats)
        {
            warning("requested summary statistics not supported by rxCube", call.=FALSE)
            method <- Recall(stats=stats, method=NULL, groups=groups)
        }
        else if(method %in% c(2, 5) && !simpleStats)
        {
            warning("requested summary statistics not supported by rxSummary", call.=FALSE)
            method <- Recall(stats=stats, method=NULL, groups=groups)
        }
    }
    method
}


# reconstruct grouping variables
rebuild_groupvars <- function(x, grps, data)
{
    if(length(x) == 1 && names(x) == ".group." && !identical(grps, ".group."))
    {
        x <- do.call(rbind, strsplit(as.character(x[[1]]), "_&&_", fixed=TRUE))
        x <- data.frame(x, stringsAsFactors=FALSE)
    }

    x <- mapply(function(x, varInfo) {
        type <- varInfo$varType
        if(type %in% c("logical", "integer", "numeric"))
            x <- as(as.character(x), type)
        else if(type %in% c("Date", "POSIXct"))
        {
            # underlying code in as.Date.numeric, as.POSIXct.numeric just adds an offset
            x <- as.numeric(as.character(x))
            class(x) <- type
        }
        else if(type %in% c("factor", "ordered") && !identical(levels(x), varInfo$levels))
            x <- factor(x, levels=varInfo$levels, ordered=(type == "ordered"))
        x
    }, x, rxGetVarInfo(data, varsToKeep=grps), SIMPLIFY=FALSE)

    names(x) <- grps
    x
}


invars <- function(exprs)
{
    sapply(exprs, function(e) {
        if(length(e) > 1)
            as.character(e[[2]])
        else ""
    })
}

