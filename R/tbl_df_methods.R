#' @include tbl_xdf.R
NULL

#' Convert a data source or tbl to a data frame
#'
#' @param x A data source object, or tbl wrapping the same.
#' @param maxRowsByCols the maximum dataset size to convert, expressed in terms of rows times columns. Defaults to NULL, meaning no maximum.
#' @param row.names,optional For compatibility with the \code{as.data.frame} generic. Not used.
#' @param ... Other arguments to \code{rxDataStep}.
#' @details
#' These are simple wrappers around \code{\link[RevoScaleR]{rxDataStep}}, with the check on the maximum table size turned off. You should ensure that you have enough memory for your data.
#'
#' \code{as.data.frame} converts a data source object (typically an xdf file, but can also be any data source type that \code{rxDataStep} supports) into a data frame. The \code{$} and \code{[[} methods extract a single column from a data source, as a vector.
#'
#' @seealso
#' \code{\link[base]{as.data.frame}}, \code{\link[dplyr]{collect}}
#' @aliases collect compute as.data.frame
#' @rdname as.data.frame
#' @export
as.data.frame.RxFileData <- function(x, maxRowsByCols=NULL, row.names=NULL, optional=TRUE, ...)
{
    rxDataStep(unTbl(x), outFile=NULL, maxRowsByCols=maxRowsByCols, ...)
}

#' @rdname as.data.frame
#' @export
collect.RxFileData <- function(x, maxRowsByCols=NULL, ...)
{
    rxDataStep(unTbl(x), outFile=NULL, maxRowsByCols=maxRowsByCols, ...)
}

#' @rdname as.data.frame
#' @export
compute.RxFileData <- collect.RxFileData

#' @param name The name of a column to extract from a data source object
#' @rdname as.data.frame
#' @export
"$.RxFileData" <- function(x, name)
{
    rxDataStep(unTbl(x), outFile=NULL, varsToKeep=name, maxRowsByCols=NULL)[[1]]
}


## rxGetVarInfo depends on Rx* sources not having a [ method
##' @export
#"[.RxFileData" <- function(x, i, j, drop=FALSE, maxRowsByCols=NULL, ...)
#{
    ## try to detect if list-style indexing used (no j argument, missing or otherwise)
    #missingI <- missing(i)
    #missingJ <- missing(j)
    #nArgs <- nargs()
    #nOtherArgs <- length(match.call(expand=FALSE)$`...`) + !missing(drop) + !missing(maxRowsByCols)
    #asList <- (nArgs - nOtherArgs <= 2) || (nArgs - nOtherArgs <= 1 && missingJ)

    #if(asList)
    #{
        ## morph to matrix-style indexing: set 2nd index (if present) to 1st, set 1st index to missing
        #if(!missingI)
            #j <- i
        #missingJ <- missingI
        #missingI <- TRUE
    #}

    ## from here on, assume matrix-style indexing (2 indices)
    #varsToKeep <- if(!missingJ)
    #{
        #nams <- names(x)
        #if(is.logical(j) || is.numeric(j))
            #varsToKeep <- nams[j]
        #else varsToKeep <- as.character(j)
    #}
    #else NULL
    #df <- rxDataStep(x, outFile=NULL, varsToKeep=varsToKeep, maxRowsByCols=maxRowsByCols, ...)
    ## must do this separately because by semantics of [, row selection expression is evaluated in calling frame
    #if(!missingI)
        #df[i, , drop=drop]
    #else df
#}


#' @rdname as.data.frame
#' @export
"[[.RxFileData" <- function(x, name, maxRowsByCols=NULL, ...)
{
    nams <- names(x)
    if(length(name) > 1)
        stop("attempt to select more than one column")
    varsToKeep <- if(is.logical(name) || is.numeric(name))
        nams[name]
    else as.character(name)

    rxDataStep(unTbl(x), outFile=NULL, varsToKeep=varsToKeep, maxRowsByCols=maxRowsByCols, ...)[[1]]
}


#' @rdname as.data.frame
#' @export
pull.RxFileData <- function(.data, var=-1)
{
    # exactly the same as for data frames
    var <- select_var(names(.data), !(!enquo(var)))
    .data[[var]]
}
