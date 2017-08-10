execByCheck <- function(execLst)
{
    ok <- sapply(execLst, function(x) x$status[[1]] == "OK")
    if(!all(ok))
    {
        errs <- sapply(execLst[!ok], function(x) x$status[[2]])
        stop("Bad result from rxExecBy: ", errs[1])
    }
}


execByResult <- function(.data, ...)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    # rxExecBy fails in local CC with relative path for HDFS data
    if(!inherits(cc, "RxDistributedHpa") && in_hdfs(.data) && substr(.data@file, 1, 1) != "/")
        .data@file <- normalizeHdfsPath(.data@file)

    execLst <- rxExecBy(.data, ...)
    execByCheck(execLst)
    lapply(execLst, "[[", "result")
}

