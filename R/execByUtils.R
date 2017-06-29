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


callExecBy <- function(.data, .func, ...)
{
    composite <- isCompositeXdf(.data)
    funcParams <- list(...)
    funcParams <- rlang::modify(funcParams, .func=.func,
        .composite=composite, .tblDir=get_dplyrxdf_dir(), .tblFunc=tbl_xdf)

    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    rxExecBy(.data, group_vars(.data), function(keys, data, .func, ...)
        .func(data, ...),
        funcParams) %>%
        execByResult
}


