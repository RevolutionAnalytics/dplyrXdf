#' @include tbl_xdf.R
NULL

#' Get the variable names or types for a RevScaleR data source
#'
#' @param x A data source object, or tbl wrapping the same.
#' @details
#' These are simple wrappers around the \code{names} and \code{rxGetVarInfo} functions. The \code{names<-} function allows renaming variables inside an Xdf file; however, the recommended method is to use the \code{\link{rename}} verb instead.
#'
#' @return
#' For \code{tbl_vars}, a character vector of variable names; for \code{tbl_types}, a named vector giving the types of each variable.
#'
#' @seealso
#' \code{\link[dplyr]{tbl_vars}} in package dplyr,
#' \code{\link{rxGetInfo}}, \code{\link{rxGetVarInfo}}
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' names(mtx)
#' tbl_vars(mtx)
#' tbl_types(mtx)
#'
#' names(mtx)[1] <- "mpg2"
#' names(mtx)
#' @aliases tbl_vars
#' @rdname tbl_vars
#' @export
tbl_vars.RxFileData <- function(x)
{
    names(x)
}


#' @rdname tbl_vars
#' @export
tbl_types <- function(x)
{
    varTypes(x, NULL)
}


#' @rdname tbl_vars
#' @export
setMethod("names", c(x="tbl_xdf"), function(x)
{
    # deal with HDFS/tbl incompatibility: convert to raw RxXdfData, then convert back
    callNextMethod(as(x, "RxXdfData"))
})


#' @rdname tbl_vars
#' @export
setMethod("names<-", c(x="tbl_xdf", value="character"), function(x, value)
{
    # deal with HDFS/tbl incompatibility: convert to raw RxXdfData, then convert back
    cls <- class(x)
    hasTbl <- x@hasTblFile
    x <- as(x, "RxXdfData")
    names(x) <- value
    x <- as(x, cls)
    x@hasTblFile <- hasTbl
    x
})


#' @rdname tbl_vars
#' @export
setMethod("names<-", c(x="grouped_tbl_xdf", value="character"), function(x, value)
{
    # deal with HDFS/tbl incompatibility: convert to raw RxXdfData, then convert back
    cls <- class(x)
    grps <- group_vars(x)
    hasTbl <- x$hasTblFile
    x <- as(x, "RxXdfData")
    names(x) <- value
    x <- as(x, cls)
    x@groups <- grps
    x@hasTblFile <- hasTbl
    x
})


# assorted unexported functions
varTypes <- function(x, vars=NULL)
{
    if(!is.null(vars) && length(vars) == 0)  # handle no-variable input
        return(character(0))

    info <- if(!in_hdfs(x))
        local_exec(rxGetVarInfo(x, varsToKeep=vars, computeInfo=FALSE))
    else rxGetVarInfo(unTbl(x), varsToKeep=vars, computeInfo=FALSE)

    sapply(info, "[[", "varType")
}


# return list of factor levels (NULL if variable is not a factor)
varLevels <- function(x, vars=NULL)
{
    if(!is.null(vars) && length(vars) == 0) # handle no-variable input
        return(list(0))

    info <- if(is.data.frame(x))
        local_exec(rxGetVarInfo(x, varsToKeep=vars, computeInfo=FALSE))
    else rxGetVarInfo(unTbl(x), varsToKeep=vars, computeInfo=FALSE)

    sapply(info, "[[", "levels", simplify=FALSE)
}


# on.exit function
deleteIfTbl <- function(data)
{
    # sanity check if passed a data frame
    if(is.data.frame(data))
        return(NULL)
    if((inherits(data, "tbl_xdf") && isTRUE(data@hasTblFile)))
        delete_xdf(data)
    NULL
}

