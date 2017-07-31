execByCheck <- function(execLst)
{
    ok <- sapply(execLst, function(x) x$status[[1]] == "OK")
    if(!all(ok))
    {
        errs <- sapply(execLst[!ok], function(x) x$status[[2]])
        stop("Bad result from rxExecBy: ", errs[1])
    }
}


execByResult <- function(...)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))
    execLst <- rxExecBy(...)
    execByCheck(execLst)
    lapply(execLst, "[[", "result")
}

