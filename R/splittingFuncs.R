# abstract out interfaces to data splitting backends for split-apply-combine strategy
callGroupedExec <- function(.data, ...)
{
    fs <- rxGetFileSystem(.data)
    cc <- rxGetComputeContext()

    # must use rxExecBy if data is on HDFS, or compute context is distributed
    if(.dxOptions$useExecBy || inherits(fs, "RxHdfsFileSystem") || inherits(cc, "RxHadoopMR"))
        callExecBy(.data, ...)
    else callSplit(.data, ...)
}


callExecBy <- function(.data, .func, ...)
{
    rxver <- packageVersion("RevoScaleR")
    if(rxver < package_version("9.1"))
        stop("rxExecBy not available in this version of RevoScaleR", call.=FALSE)

    composite <- isCompositeXdf(.data)
    funcParams <- list(...)
    funcParams <- rlang::modify(funcParams, .func=.func,
        .composite=composite, .tblDir=get_dplyrxdf_dir())

    # call unTbl to handle HDFS/tbl incompatibility
    execByResult(unTbl(.data), group_vars(.data), function(keys, data, .func, ...)
        .func(data, ...), funcParams)
}


callSplit <- function(.data, .func, ...)
{
    composite <- isCompositeXdf(.data)
    xdflst <- splitGroups(.data)
    on.exit(deleteIfTbl(xdflst))

    rxExec(.func, rxElemArg(xdflst), ...,
        .composite=composite, .tblDir=get_dplyrxdf_dir())
}

