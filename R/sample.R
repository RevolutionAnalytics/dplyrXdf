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
        sampleExecBy(tbl, size, frac)
    else sampleSplit(tbl, size, frac)
}


sampleExecBy <- function(data, size, frac)
{
    grps <- group_vars(data)
    execByResult(rxExecBy(data, grps, sampleBase,
        list(size=size, frac=frac, tblDir=get_dplyrxdf_dir(), tblFunc=tbl_xdf)))
}


sampleSplit <- function(data, size, frac)
{
    xdflst <- splitGroups(data)
    on.exit(deleteIfTbl(xdflst))
    outlst <- createSplitOutput(xdflst, tbl_xdf(data))
    rxExec(sampleBase,
        data=rxElemArg(xdflst), output=rxElemArg(outlst), size=size, frac=frac)
}


sampleBase <- function(data, output=tblFunc(tmpdir=tblDir, fileext=".xdf"), size, frac, tblDir, tblFunc)
{
    n <- nrow(data)
    if(frac)
        size <- round(size * n)
    if(size > n)
        stop("sample size must be less than or equal to number of rows in group")
    rxDataStep(data, output, (.rxStartRow + seq_len(.rxNumRows) - 1) %in% .sel, transformObjects=list(.sel=sel))
}
