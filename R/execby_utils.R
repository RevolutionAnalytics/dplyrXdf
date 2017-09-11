execByCheck <- function(execLst)
{
    ok <- sapply(execLst, function(x) x$status[[1]] == "OK")
    if(!all(ok))
    {
        errs <- sapply(execLst[!ok], function(x) x$status[[2]])
        stop("bad result from rxExecBy: ", errs[1])
    }
}


execByResult <- function(.data, ...)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    execLst <- callRx("rxExecBy", list(.data, ...))
    execByCheck(execLst)
    lapply(execLst, "[[", "result")
}

