#' @include summarise_xdf.R
NULL


smryRxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
        return(smryRxSummary(data, grps, stats, exprs, rxArgs))

    callFunc <- if(useExecBy(data)) callExecBy else callSplit

    out <- callFunc(data, smryRxSummaryWithGroupvars, grps=grps, stats=stats, exprs=exprs, rxArgs=rxArgs) %>%
        combineGroups(tbl_xdf(data), NULL)
}


smryRxSummaryWithGroupvars <- function(data, grps, stats, exprs, rxArgs=NULL, ...)
{
    gvars <- rxDataStep(unTbl(data), varsToKeep=grps, numRows=1)
    smry <- smryRxSummary(data, NULL, stats, exprs, rxArgs)
    cbind(gvars, smry)
}


smryRxSplitDplyr <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
    {
        on.exit(deleteIfTbl(data))

        arglst <- list(data, maxRowsByCols=NULL)
        if(!is.null(rxArgs))
            arglst <- rlang::modify(arglst, !!!rxArgs)
        return(callRx("rxDataStep", arglst) %>%
            summarise(!!!exprs))
    }

    callFunc <- if(useExecBy(data)) callExecBy else callSplit

    callFunc(data, smryDplyrWithGroupvars, grps=grps, stats=stats, exprs=exprs, rxArgs=rxArgs) %>%
        combineGroups(tbl_xdf(data), NULL)
}


smryDplyrWithGroupvars <- function(data, grps, exprs, rxArgs=NULL, ...)
{
    gvars <- rxDataStep(unTbl(data), varsToKeep=grps, numRows=1)
    arglst <- list(data, maxRowsByCols=NULL)

    if(!is.null(rxArgs))
        arglst <- rlang::modify(arglst, !!!rxArgs)
    smry <- callRx("rxDataStep", arglst) %>%
        summarise(!!!exprs)
        
    cbind(gvars, smry)
}


