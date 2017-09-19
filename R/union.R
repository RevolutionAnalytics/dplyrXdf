#' Set operations on data sources
#'
#' @param x,y Data sources.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param ... Not currently used.
#'
#' @details
#' Currently, only \code{union} and \code{union_all} are supported for RevoScaleR data sources. The code uses \code{rxDataStep(append="rows")} to do the union; this can be much faster than using \code{rxMerge(type="union")}.
#'
#' @return
#' An object representing the joined data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{setops}} and \code{\link[dplyr]{bind_rows}} in package dplyr, \code{rbind}
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' tbl <- union(mtx, mtx)
#' nrow(tbl)
#'
#' # union_all doesn't remove duplicated rows
#' tbl2 <- union_all(mtx, mtx)
#' nrow(tbl2)
#' @aliases setops union union_all intersect setdiff setequal
#' @name setops
NULL

#' @rdname setops
#' @export
union_all.RxFileData <- function(x, y, .outFile=tbl_xdf(x), .rxArgs)
{
    # need to create a new copy of x?
    # tbl -> tbl: ok
    # xdf -> tbl: copy
    # txt -> tbl: copy
    # tbl -> xdf: copy
    # xdf -> xdf: copy
    # txt -> xdf: copy
    copyBaseTable <- function(data, output)
    {
        tblInput <- inherits(data, "tbl_xdf")
        xdfInput <- inherits(data, "RxXdfData") && !tblInput
        txtInput <- inherits(data, "RxFileData") && !xdfInput && !tblInput
        tblOutput <- inherits(output, "tbl_xdf")
        xdfOutput <- !tblOutput && is.character(output)

        # step through all possible combinations
        if(tblInput && tblOutput)
            return(data)
        else if(xdfInput && tblOutput)  # excludes data is tbl_xdf
        {
            output <- tbl_xdf(data)
            file.copy(data@file, output@file, overwrite=TRUE)
            return(output)
        }
        else if(txtInput && tblOutput)
        {
            return(as(data, "tbl_xdf"))
        }
        else if((xdfInput || tblInput) && xdfOutput)  # also includes data is tbl_xdf
        {
            on.exit(deleteIfTbl(data))
            file.copy(data@file, output, overwrite=TRUE)
            return(RxXdfData(output))
        }
        else if(txtInput && xdfOutput)
        {
            return(rxImport(data, output, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE))
        }
        else stop("error handling base table in union", call.=TRUE)
    }

    stopIfHdfs(x, "union_all not supported on HDFS")
    stopIfHdfs(y, "union_all not supported on HDFS")

    grps <- group_vars(x)

    # if output is a data frame: convert x and y to df, run dplyr::union_all
    if(!missing(.outFile) && is.null(.outFile))
        return(union_all(as.data.frame(x), as.data.frame(y)))

    # use rxDataStep to append y to x, faster than rxMerge(type="union")
    # first, make a copy of x if necessary
    x <- copyBaseTable(x, .outFile)

    # if y points to same file as x, also make a copy
    # should only happen with union_all(x, x)
    if(inherits(y, "RxFileData") && y@file == x@file)
    {
        oldFile <- y@file
        y <- newTbl(y)
        file.copy(oldFile, y@file)
        on.exit(file.remove(y@file))
    }

    arglst <- list(y, append="rows")
    arglst <- doExtraArgs(arglst, x, enexpr(.rxArgs), x)
    x <- callRx("rxDataStep", arglst)

    simpleRegroup(x, grps)
}


#' @rdname setops
#' @export
union.RxFileData <- function(x, y, .outFile=tbl_xdf(x), .rxArgs, ...)
{
    stopIfHdfs(x, "union not supported on HDFS")
    stopIfHdfs(y, "union not supported on HDFS")

    union_all(x, y, .rxArgs=.rxArgs) %>%
        distinct(.outFile=.outFile)
}

