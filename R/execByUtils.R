execByCheck <- function(execLst)
{
    allOk <- all(sapply(execLst, function(x) x$status[[1]] == "OK"))
    if(!allOk)
        stop("Bad result from rxExecBy")
}


execByResult <- function(execLst)
{
    execByCheck(execLst)
    lapply(execLst, "[[", "result")
}

