#' @include tbl_xdf.R
NULL

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


#' @export
"$.RxFileData" <- function(x, name)
{
    rxDataStep(x, outFile=NULL, varsToKeep=name, maxRowsByCols=NULL)[[1]]
}


#' @export
"[.RxFileData" <- function(x, i, j, drop=FALSE, maxRowsByCols=NULL, ...)
{
    # try to detect if list-style indexing used (no j argument, missing or otherwise)
    missingI <- missing(i)
    missingJ <- missing(j)
    nArgs <- nargs()
    nOtherArgs <- length(match.call(expand=FALSE)$`...`) + !missing(drop) + !missing(maxRowsByCols)
    asList <- (nArgs - nOtherArgs <= 2) || (nArgs - nOtherArgs <= 1 && missingJ)

    if(asList)
    {
        # morph to matrix-style indexing: set 2nd index (if present) to 1st, set 1st index to missing
        if(!missingI)
            j <- i
        missingJ <- missingI
        missingI <- TRUE
    }

    # from here on, assume matrix-style indexing (2 indices)
    varsToKeep <- if(!missingJ)
    {
        nams <- names(x)
        if(is.logical(j) || is.numeric(j))
            varsToKeep <- nams[j]
        else varsToKeep <- as.character(j)
    }
    else NULL
    df <- rxDataStep(x, outFile=NULL, varsToKeep=varsToKeep, maxRowsByCols=maxRowsByCols, ...)
    # must do this separately because by semantics of [, row selection expression is evaluated in calling frame
    if(!missingI)
        df[i, , drop=drop]
    else df
}


#' @export
"[[.RxFileData" <- function(x, name, outFile=NULL, maxRowsByCols=NULL, ...)
{
    nams <- names(x)
    if(length(name) > 1)
        stop("attempt to select more than one column")
    varsToKeep <- if(is.logical(name) || is.numeric(name))
        nams[name]
    else as.character(name)
    rxDataStep(x, outFile=outFile, varsToKeep=varsToKeep, maxRowsByCols=maxRowsByCols, ...)[[1]]
}


#' @export
subset.RxFileData <- function(.data, subset=NULL, select=NULL, outFile=NULL, maxRowsByCols=NULL, ...)
{
    rowSelection <- substitute(subset)
    varsToKeep <- if(!missing(select))
    {
        nams <- names(.data)
        nl <- as.list(seq_along(nams))
        names(nl) <- nams
        select <- eval(substitute(select), nl, parent.frame())
        if(is.logical(select) || is.numeric(select))
            varsToKeep <- nams[select]
        else varsToKeep <- as.character(select)
    }
    else NULL
    rxDataStep(.data, outFile=outFile, rowSelection=rowSelection, varsToKeep=varsToKeep,
               maxRowsByCols=NULL, ...)
}


