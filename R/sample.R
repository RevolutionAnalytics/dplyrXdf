#' @export
sample_n.RxXdfData <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    if(replace)
        warning("sampling with replacement not supported for Xdf files")
    if(!is.null(weight))
        warning("weighted sampling not supported for Xdf files")

    sampleBase(tbl, tbl_xdf(tbl), size, FALSE)
}


#' @export
sample_frac.RxXdfData <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    if(replace)
        warning("sampling with replacement not supported for Xdf files")
    if(!is.null(weight))
        warning("weighted sampling not supported for Xdf files")

    sampleBase(tbl, tbl_xdf(tbl), size, TRUE)
}


#' @export
sample_n.grouped_tbl_xdf <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    grps <- group_vars(tbl)
    outlst <- sampleGroupedXdf(tbl, size, replace, weight, FALSE)
    combineGroups(outlst, tbl_xdf(tbl), grps)
}


#' @export
sample_frac.grouped_tbl_xdf <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    grps <- group_vars(tbl)
    outlst <- sampleGroupedXdf(tbl, size, replace, weight, TRUE)
    combineGroups(outlst, tbl_xdf(tbl), grps)
}


sampleGroupedXdf <- function(data, size, replace=FALSE, weight=NULL, frac)
{
    if(replace)
        warning("sampling with replacement not supported for Xdf files")
    if(!is.null(weight))
        warning("weighted sampling not supported for Xdf files")

    if(.dxOptions$useExecBy)
        sampleGroupedExecBy(data, size, frac)
    else sampleGroupedSplit(data, size, frac)
}


sampleGroupedExecBy <- function(data, size, frac)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    grps <- group_vars(data)
    tblDir <- get_dplyrxdf_dir()
    execByResult(rxExecBy(data, grps, function(keys, data, sampleFunc, ...) sampleFunc(data, ...),
        list(sampleFunc=sampleBase, size=size, frac=frac, tblDir=tblDir, tblFunc=tbl_xdf)))
}


sampleGroupedSplit <- function(data, size, frac)
{
    xdflst <- splitGroups(data)
    on.exit(deleteIfTbl(xdflst))
    outlst <- createSplitOutput(xdflst, tbl_xdf(data))
    rxExec(sampleBase,
        data=rxElemArg(xdflst), output=rxElemArg(outlst), size=size, frac=frac)
}


sampleBase <- function(data, output=tblFunc(file=tempfile(tmpdir=tblDir, fileext=".xdf")), size, frac, tblDir, tblFunc)
{
    n <- nrow(data)
    if(frac)
        size <- round(size * n)
    if(size > n)
        stop("sample size must be less than or equal to number of rows in group")
    if(size < 1)
        stop("sample size must be at least 1")
    sel <- sample.int(n, size=size)
    rxDataStep(data, output, rowSelection=(.rxStartRow + seq_len(.rxNumRows) - 1) %in% .sel,
        transformObjects=list(.sel=sel))
}
