#' Convert columns in an Xdf file to factor
#'
#' @param .data A data source.
#' @param ... variables to convert to factors.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#'
#' @details
#' The selector functions listed in \code{\link[dplyr]{select}} also work with \code{factorise}. In addition, you can use the following:
#' \itemize{
#'  \item \code{all_character()}: selects all character variables
#'  \item \code{all_integer()}: selects all integer variables, ie those of type \code{"logical"} and \code{"integer"}
#'  \item \code{all_numeric()}: selects all numeric variables, ie those of type \code{"numeric"}, \code{"Date"}, \code{"POSIXct"}, \code{"logical"} and \code{"integer"}
#' }
#' If no variables are specified, all character variables will be converted to factors.
#'
#' This is a shell around \code{\link[RevoScaleR]{rxFactors}}, which is the standard RevoScaleR function for factor manipulation. Note that for performance reasons, factors created by \code{factorise} are not sorted; instead, the ordering of their levels will be determined by the order in which they are encountered in the data.
#'
#' @seealso
#' \code{\link[RevoScaleR]{rxFactors}}, \code{\link{factor}}
#'
#' \code{\link{chol}}, \code{\link{qr}}, \code{\link{svd}} for the other meaning of factorise
#' @rdname factorise
#' @export
factorise <- function(.data, ...)
{
    factorise_(.data, .dots=lazyeval::lazy_dots(...))
}


#' @rdname factorise
#' @export
factorize <- factorise


#' @rdname factorise
#' @export
factorise_ <- function(.data, ..., .dots)
{
    UseMethod("factorise_")
}


#' @rdname factorise
#' @export
factorize_ <- factorise_


#' @rdname factorise
#' @export
factorise_.RxXdfData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    grps <- groups(.data)
    types <- varTypes(.data)
    vars <- factorise_vars(types, exprs)

    oldfile <- tblFile(.data)
    if(hasTblFile(.data))
        on.exit(file.remove(oldfile))

    cl <- quote(rxFactors(.data, vars, outFile=newTblFile()))
    cl[names(rxArgs)] <- rxArgs
    
    # rxFactors is noisy when given already-existing factors; shut it up
    .data <- suppressWarnings(tbl(eval(cl), hasTblFile=TRUE))
    if(!is.null(grps))
        group_by_(.data, .dots=grps)
    else .data
}


#' @rdname factorise
#' @export
factorise_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    grps <- groups(.data)
    types <- varTypes(.data)
    vars <- factorise_vars(types, dots)
    
    colInfo <- sapply(names(.data), function(x) {
        list(type=if(x %in% vars) "factor" else types[x])
    }, simplify=FALSE)

    oldfile <- tblFile(.data)
    if(hasTblFile(.data))
        on.exit(file.remove(oldfile))

    cl <- quote(rxImport(.data, newTblFile(), colInfo=colInfo))
    cl[names(rxArgs)] <- rxArgs

    .data <- tbl(eval(cl), hasTblFile=TRUE)
    if(!is.null(grps))
        group_by_(.data, .dots=grps)
    else .data
}


factorise_vars <- function(types, args)
{
    # extra selection functions
    selectionFuncs <- list(
        `all_character()`=function(...) vars[types %in% "character"],
        `all_numeric()`=function(...) vars[types %in% c("numeric", "logical", "integer")],
        `all_integer()`=function(...) vars[types %in% c("logical", "integer")]
    )

    vars <- names(types)
    if(length(args) == 0)
        return(selectionFuncs$all_character())

    newArgs <- list()
    for(i in seq_along(args))
    {
        arg <- args[[i]]
        i <- match(deparse(arg), names(selectionFuncs), nomatch=0)
        if(i > 0)
        {
            arg <- as.lazy_dots(selectionFuncs[[i]]())
            newArgs <- c(newArgs, arg)
        }
        else newArgs <- c(newArgs, list(arg))
    }
    select_vars_(vars, newArgs)
}


