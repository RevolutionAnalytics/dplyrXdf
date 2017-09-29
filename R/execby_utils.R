execByCheck <- function(execLst)
{
    ok <- sapply(execLst, function(x) x$status[[1]] == "OK")
    if(!all(ok))
    {
        errs <- sapply(execLst[!ok], function(x) x$status[[2]])
        stop("bad result from rxExecBy: ", errs[1])
    }
}


execByResult <- function(.data, vars, ...)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    # workaround crazy rxExecBy issue
    vars <- unname(vars)

    execLst <- callRx("rxExecBy", list(.data, vars, ..., filterFunc=function(x) x))
    execByCheck(execLst)
    lapply(execLst, "[[", "result")
}

