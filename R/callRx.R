# convert tbl_xdf -> RxXdfData, relative -> absolute paths in HDFS before calling an rx* function
# - because Spark/Hadoop compute context is broken
callRx <- function(func="rxDataStep", arglst, asTbl=inherits(arglst$outFile, "tbl_xdf"))
{
    force(asTbl)
    arglst <- lapply(arglst, function(arg)
    {
        if(in_hdfs(arg))
        {
            # change relative to absolute paths on HDFS
            if(inherits(arg, "RxXdfData"))
                arg <- modifyXdf(arg, file=normalizeHdfsPath(arg@file)) # will also strip tbl_xdf class
            else if(inherits(arg, "RxFileData"))
                arg@file <- normalizeHdfsPath(arg@file)
        }
        arg
    })

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

