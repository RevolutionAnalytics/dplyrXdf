# abstract out interfaces to data splitting backends for split-apply-combine strategy

callExecBy <- function(.data, .func, ...)
{
    rxver <- packageVersion("RevoScaleR")
    if(rxver < package_version("9.1"))
        stop("rxExecBy not available in this version of RevoScaleR", call.=FALSE)

    composite <- isCompositeXdf(.data)
    funcParams <- list(...)
    funcParams <- rlang::modify(funcParams, .func=.func,
        .composite=composite, .tblDir=get_dplyrxdf_dir())

    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    # call unTbl to handle HDFS/tbl incompatibility
    rxExecBy(unTbl(.data), group_vars(.data), function(keys, data, .func, ...)
        .func(data, ...),
        funcParams) %>%
        execByResult
}


callSplit <- function(.data, .func, ...)
{
    composite <- isCompositeXdf(.data)
    xdflst <- splitGroups(.data)
    on.exit(deleteIfTbl(xdflst))

    rxExec(.func, rxElemArg(xdflst), ...,
        .composite=composite, .tblDir=get_dplyrxdf_dir())
}


useExecBy <- function(.data)
{
    fs <- rxGetFileSystem(.data)
    cc <- rxGetComputeContext()
    # must use rxExecBy if data is on HDFS, or compute context is distributed
    .dxOptions$useExecBy || inherits(fs, "RxHdfsFileSystem") || inherits(cc, "RxHadoopMR")
}
