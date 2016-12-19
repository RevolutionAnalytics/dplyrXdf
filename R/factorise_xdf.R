#' Convert columns in an Xdf file to factor
#'
#' @param .data A data source.
#' @param ... Variables to convert to factors.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
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
#' You can specify the levels for a variable by specifying them as the value of the argument. For example, \code{factorise(*, x = c("a","b","c"))} will turn the variable \code{x} into a factor with three levels \code{a}, \code{b} and \code{c}. Any values that don't match the set of levels will be turned into NAs. In particular, this means you should include the existing levels for variables that are already factors.
#'
#' For performance reasons, factors created by \code{factorise} are not sorted; instead, the ordering of their levels will be determined by the order in which they are encountered in the data.
#'
#' The method for \code{RxXdfData} objects is a shell around \code{\link[RevoScaleR]{rxFactors}}, which is the standard RevoScaleR function for factor manipulation. For \code{RxFileData} objects, the method calls \code{\link[RevoScaleR]{rxImport}} with an appropriately constructed \code{colInfo} argument.
#'
#' @return
#' An object representing the returned data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[RevoScaleR]{rxFactors}}, \code{\link[RevoScaleR]{rxImport}}, \code{\link{factor}}
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
factorise_.RxXdfData <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    grps <- groups(.data)
    types <- varTypes(.data)
    vars <- factoriseVars(types, lapply(exprs, lazyeval::as.lazy, dots$env))

    factorInfo <- c(
        sapply(vars$blankArgs, function(nam) list(varName=nam), simplify=FALSE),
        sapply(names(vars$newlevelArgs), function(nam) {
            levs <- vars$newlevelArgs[[nam]]
            if(types[nam] == "factor")
                list(newLevels=levs)
            else list(levels=levs)
        }, simplify=FALSE)
    )

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    .outFile <- createOutput(.data, .outFile)
    cl <- quote(rxFactors(.data, factorInfo, outFile=.outFile, overwrite=TRUE))
    cl[names(.rxArgs)] <- .rxArgs

     # rxFactors is noisy when given already-existing factors; shut it up
    .data <- suppressWarnings(eval(cl))

    # rxFactors won't preserve class of output object, unlike rxDataStep
    if(inherits(.outFile, "tbl_xdf"))
    {
        .data <- as(.data, "tbl_xdf")
        .data@hasTblFile <- TRUE
    }
    simpleRegroup(.data, grps)
}


#' @rdname factorise
#' @export
factorise_.RxFileData <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    grps <- groups(.data)
    types <- varTypes(.data)
    vars <- factoriseVars(types, lapply(exprs, lazyeval::as.lazy, dots$env))

    colInfo <- sapply(names(.data), function(x) {
        if(x %in% (vars$blankArgs))
            list(type="factor")
        else if(x %in% names(vars$newlevelArgs))
            list(type="factor", levels=as.character(vars$newlevelArgs[[x]]))
        else list(type=types[x])
    }, simplify=FALSE)

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    .outFile <- createOutput(.data, .outFile)
    cl <- quote(rxImport(.data, .outFile, colInfo=colInfo, overwrite=TRUE))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data, grps)
}


factoriseVars <- function(types, args)
{
    .factoriseEnv$.varTypes <- types
    on.exit(.factoriseEnv$.varTypes <- NULL)
    
    # variable selector functions for factoring
    selectionFuncs <- c("all_character", "all_numeric", "all_integer",
                        "starts_with", "ends_with", "contains", "matches", "num_range", "one_of", "everything")

    if(length(args) == 0)
        return(list(blankArgs=all_character(types), newlevelArgs=NULL))

    # do blank arguments and arguments specifying new factor levels separately
    # unlike select, named arguments always treated as specifying factor levels, not indices
    # blank arguments can also include variable selector functions
    isBlankArg <- sapply(names(args), function(n) {
        e <- args[[n]]$expr
        deparse(e) == n || (is.call(e) && deparse(e[[1]]) %in% selectionFuncs)
    })

    blankArgs <- select_vars_(names(types), args[isBlankArg])
    names(blankArgs) <- sapply(blankArgs, as.character)
    newlevelArgs <- lapply(args[!isBlankArg], lazyeval::lazy_eval)

    list(blankArgs=blankArgs, newlevelArgs=newlevelArgs)
}


# extra selection functions
#' @rdname factorise
#' @export
all_character <- function(vars=.varTypes)
{
    factorise_sel(vars, "character")
}


#' @rdname factorise
#' @export
all_integer <- function(vars=.varTypes)
{
    factorise_sel(vars, c("logical", "integer"))
}


#' @rdname factorise
#' @export
all_numeric <- function(vars=.varTypes)
{
    factorise_sel(vars, c("numeric", "logical", "integer"))
}


factorise_sel <- function(vars, target)
{
    n <- which(vars %in% target)
    if(length(n) == 0)
        -seq_len(length(vars))
    else n
}


# environment for storing var names/types for factoring
.factoriseEnv <- new.env()
environment(factoriseVars) <- .factoriseEnv
environment(all_character) <- .factoriseEnv
environment(all_integer) <- .factoriseEnv
environment(all_numeric) <- .factoriseEnv

