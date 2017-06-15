#' @include summarise_xdf.R
#' @include summarise_xdf_summary.R
NULL


smryRxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
        return(smryRxSummary(data, grps, stats, exprs, rxArgs))

    outlst <- if(.dxOptions$useExecBy)
        smryExecBy(data, grps, stats, exprs, rxArgs)
    else
    {
        xdflst <- splitGroups(data)
        rxExec(smryRxSummaryWithGroupvars, data=rxElemArg(xdflst), grps, stats, exprs, rxArgs,
            execObjects=c("deleteIfTbl", "smryRxSummary.RxFileData", "rxSummaryStat", "selectCol", "setSmryClasses"),
            packagesToLoad="dplyr")
    }
    combineGroups(outlst, tbl_xdf(data), NULL)
}


smryRxSummaryWithGroupvars <- function(data, grps, stats, exprs, rxArgs)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    smry <- smryRxSummary.RxFileData(data, NULL, stats, exprs, rxArgs, dfOut=TRUE)
    cbind(gvars, smry, stringsAsFactors=FALSE)
}


smryExecBy <- function(data, grps, stats, exprs, rxArgs)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    execByResult(rxExecBy(data, grps, function(keys, data, grps, stats, exprs, rxArgs)
        {
            require(dplyr)
            gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
            smry <- dplyrXdf:::smryRxSummary.RxFileData(data, NULL, stats, exprs, rxArgs, dfOut=TRUE)
            cbind(gvars, smry, stringsAsFactors=FALSE)
        },
        list(grps=grps, stats=stats, exprs=exprs, rxArgs=rxArgs)))
}


smryRxSplitDplyr <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
    {
        oldData <- data
        on.exit(deleteIfTbl(oldData))

        cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
        if(!is.null(rxArgs))
            cl <- rlang::lang_modify(cl, rlang::splice(rxArgs))

        return(eval(cl) %>%
               dplyr::summarise(!!!exprs))
    }

    outlst <- if(.dxOptions$useExecBy)
        smryDplyrExecBy(data, grps, stats, exprs, rxArgs)
    else
    {
        xdflst <- splitGroups(data)
        rxExec(smryDplyrWithGroupvars, data=rxElemArg(xdflst), grps, exprs, rxArgs,
            execObjects="deleteIfTbl", packagesToLoad="dplyr")
    }
    combineGroups(outlst, tbl_xdf(data), NULL)
}


smryDplyrWithGroupvars <- function(data, grps, exprs, rxArgs)
{
    oldData <- data
    on.exit(deleteIfTbl(oldData))

    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
    if(!is.null(rxArgs))
        cl <- rlang::lang_modify(cl, rlang::splice(rxArgs))
    smry <- eval(cl) %>%
        dplyr::summarise(!!!exprs)
        
    cbind(gvars, smry, stringsAsFactors=FALSE)
}


smryDplyrExecBy <- function(data, grps, stats, exprs, rxArgs)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    execByResult(rxExecBy(data, grps, function(keys, data, grps, stats, exprs, rxArgs)
        {
            require(dplyr)
            gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
            cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
            if(!is.null(rxArgs))
                cl <- rlang::lang_modify(cl, rlang::splice(rxArgs))
            smry <- eval(cl) %>%
                dplyr::summarise(!!!exprs)

            cbind(gvars, smry, stringsAsFactors=FALSE)
        },
        list(grps=grps, stats=stats, exprs=exprs, rxArgs=rxArgs)))
}

