execByCheck <- function(execLst)
{
    allOk <- all(sapply(execLst, "[[", "status") == "OK")
    if(!allOk)
        stop("Bad result from rxExecBy")
}


execByResult <- function(execLst)
{
    execByCheck(execLst)
    lapply(execLst, "[[", "result")
}

