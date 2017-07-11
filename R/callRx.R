# convert all tbl_xdf's to RxXdfData before calling an rx* function, because Spark/Hadoop compute context is broken
# afterwards, convert back
callRx <- function(func="rxDataStep", arglst, asTbl=NULL)
{
    if(is.null(asTbl))
        asTbl <- inherits(arglst$outFile, "tbl_xdf")

    arglst <- lapply(arglst, function(x) {
        if(inherits(x, "tbl_xdf"))
            as(x, "RxXdfData")
        else x
    })

    out <- do.call(func, arglst, envir=parent.frame(2))
    if(asTbl)
        as(out, "tbl_xdf")
    else out
}

