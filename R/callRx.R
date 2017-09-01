# convert all tbl_xdf's to RxXdfData before calling an rx* function, because Spark/Hadoop compute context is broken
# afterwards, convert back
callRx <- function(func="rxDataStep", arglst, asTbl=inherits(arglst$outFile, "tbl_xdf"))
{
    arglst <- lapply(arglst, unTbl)
    out <- do.call(func, arglst, envir=parent.frame(2))

    # rxMerge returns NULL on Spark, use outFile arg from arglst
    if(is.null(out))
        out <- arglst$outFile

    if(is.null(out))
        stop("unable to find output from ", func)

    if(asTbl)
        as(out, "tbl_xdf")
    else out
}


# remove tbl_xdf class if dealing with HDFS
unTbl <- function(.data)
{
    if(inherits(.data, "tbl_xdf") && in_hdfs(.data))
        as(.data, "RxXdfData")
    else .data
}
