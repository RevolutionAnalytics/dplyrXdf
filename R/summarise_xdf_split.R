#' @include summarise_xdf.R
NULL


smryRxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
        return(smryRxSummary(data, grps, stats, exprs, rxArgs))

    callGroupedExec(data, NULL, smryRxSummaryWithGroupvars, grps=grps, stats=stats, exprs=exprs, rxArgs=rxArgs,
        distribFuncList=list(buildSmryFormulaRhs=buildSmryFormulaRhs, setSmryClasses=setSmryClasses))
}


smryRxSummaryWithGroupvars <- function(data, grps, stats, exprs, rxArgs=NULL, ...)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
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

    callGroupedExec(data, NULL, smryDplyrWithGroupvars, grps=grps, stats=stats, exprs=exprs, rxArgs=rxArgs)
}


smryDplyrWithGroupvars <- function(data, grps, exprs, rxArgs=NULL, ...)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    arglst <- list(data, maxRowsByCols=NULL)

    if(!is.null(rxArgs))
        arglst <- rlang::modify(arglst, !!!rxArgs)

    smry <- dplyr::summarise(do.call("rxDataStep", arglst), !!!exprs)
        
    cbind(gvars, smry)
}


