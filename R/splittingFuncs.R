# abstract out interfaces to data splitting backends for split-apply-combine strategy
callGroupedExec <- function(.data, .output, ...)
{
    gvars <- group_vars(.data)
    if(length(gvars) == 0)
        stop("no groups defined for splitting")

    fs <- rxGetFileSystem(.data)
    cc <- rxGetComputeContext()

    # must use rxExecBy if data is on HDFS, or compute context is distributed
    outlst <- if(.dxOptions$useExecBy || inherits(fs, "RxHdfsFileSystem") || inherits(cc, "RxHadoopMR"))
        callExecBy(.data, ...)
    else callSplit(.data, ...)

    if(is.character(.output))
        .output <- modifyXdf(.data, file=.output)
    combineGroups(outlst, .output, gvars)
}


callExecBy <- function(.data, .func, ...)
{
    rxver <- packageVersion("RevoScaleR")
    if(rxver < package_version("9.1"))
        stop("rxExecBy not available in this version of RevoScaleR")

    composite <- isCompositeXdf(.data)
    funcParams <- rlang::modify(list(...), .func=.func,
        .composite=composite, .tblDir=get_dplyrxdf_dir(rxGetFileSystem(.data)))

    execByResult(unTbl(.data), group_vars(.data), function(keys, data, .func, ...)
        .func(data, ...), funcParams)
}


callSplit <- function(.data, .func, ...)
{
    if(in_hdfs(.data))
        stop("cannot use manual splitting for data in HDFS")

    composite <- isCompositeXdf(.data)
    xdflst <- splitGroups(.data)
    on.exit(deleteIfTbl(xdflst))

    rxExec(.func, rxElemArg(xdflst), ...,
        .composite=composite, .tblDir=get_dplyrxdf_dir())
}

