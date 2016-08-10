#' @include summarise_xdf.R
#' @include summarise_xdf_summary.R
NULL


smry_rxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
        return(smry_rxSummary(data, grps, stats, exprs, rxArgs))

    outSource <- tblSource(data)
    xdflst <- split_groups(data, outSource)
    xdflst <- rxExec(smry_rxSummary_with_groupvars, data=rxElemArg(xdflst), grps, stats, exprs, rxArgs,
        packagesToLoad="dplyrXdf")
    combine_groups(xdflst, outSource, NULL)
}


smry_rxSummary_with_groupvars <- function(data, grps, stats, exprs, rxArgs)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    smry <- smry_rxSummary(data, NULL, stats, exprs, rxArgs, dfOut=TRUE)
    cbind(gvars, smry, stringsAsFactors=FALSE)
}


smry_rxSplit_dplyr <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
    {
        invars <- invars(exprs)
        cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
        cl[names(rxArgs)] <- rxArgs
        return(eval(cl) %>%
               dplyr::summarise_(.dots=exprs) %>%
               rxDataStep(tblSource(data), overwrite=TRUE))
    }

    outSource <- tblSource(data)
    xdflst <- split_groups(data, outSource)
    xdflst <- rxExec(smry_dplyr_with_groupvars, data=rxElemArg(xdflst), grps, exprs, rxArgs,
        execObjects="invars", packagesToLoad="dplyr")
    combine_groups(xdflst, outSource, NULL)
}


smry_dplyr_with_groupvars <- function(data, grps, exprs, rxArgs)
{
    invars <- invars(exprs)
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
    cl[names(rxArgs)] <- rxArgs
    exprs <- lazyeval::as.lazy_dots(exprs, attr(exprs, "env"))
    smry <- eval(cl) %>%
        dplyr::summarise_(.dots=exprs)
        
    cbind(gvars, smry, stringsAsFactors=FALSE)
}
