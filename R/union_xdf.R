# duplicate the generic from dplyr 0.5, to allow install with dplyr <= 0.4.3
#' @export
union_all <- function(x, y, ...)
UseMethod("union_all")


#' @export
union_all.RxFileData <- function(x, y, .output, .rxArgs, ...)
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
        # step through all possible combinations
        if(inherits(data, "tbl_xdf") && is.na(output))
            return(data)
        else if(inherits(data, "RxXdfData") && is.na(output))  # excludes data is tbl_xdf
        {
            oldData <- data
            if(hasTblFile(data))
                on.exit(deleteTbl(oldData))
            output <- tbl(newTbl(data), hasTblFile=TRUE)
            file.copy(data@file, output@file)
            return(output)
        }
        else if(inherits(data, "RxFileData") && is.na(output))
        {
            output <- tbl(data, stringsAsFactors=FALSE)
            return(output)
        }
        else if(inherits(data, "RxXdfData") && is.character(output))  # also includes data is tbl_xdf
        {
            oldData <- data
            if(hasTblFile(data))
                on.exit(deleteTbl(oldData))
            file.copy(data@file, output)
            return(RxXdfData(output))
        }
        else if(inherits(data, "RxFileData") && is.character(output))
        {
            outFile <- tbl(newTbl(data), hasTblFile=TRUE)
            return(tbl(data, file=output@file))
        }
        else stop("error handling base table in union", call.=TRUE)
    }

    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")

    dots <- lazyeval::lazy_dots(...)
    dots <- rxArgs(dots)

    exprs <- dots$exprs
    if(missing(.output)) .output <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs
    grps <- groups(x)

    # if output is a data frame: convert x and y to df, run dplyr::union_all
    if(is.null(.output))
    {
        if(.dxOptions$dplyrVersion < package_version("0.5"))
            stop("cannot output directly to data frame with dplyr version < 0.5")
        return(union_all(as.data.frame(x), as.data.frame(y)))
    }

    # use rxDataStep to append y to x, faster than rxMerge(type="union")
    # first, make a copy of x if necessary
    x <- copyBaseTable(x, .output)

    # if y points to same file as x, also make a copy
    # should only happen with union_all(x, x)
    if(inherits(y, "RxFileData") && y@file == x@file)
    {
        oldY <- y
        y <- newTbl(y)
        file.copy(oldY@file, y@file)
        on.exit(file.remove(y@file))
    }

    cl <- quote(rxDataStep(y, x, append="rows"))
    cl[names(.rxArgs)] <- .rxArgs

    x <- eval(cl)
    simpleRegroup(x, grps)
}


#' @export
union.RxFileData <- function(x, y, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")

    # call union_all.RxFileData explicitly to allow use in dplyr < 0.5
    union_all.RxFileData(x, y, ...) %>% distinct
}

