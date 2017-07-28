#' @include summarise_xdf.R
NULL


smryRxSummary2 <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    # should always have groups
    if(length(grps) == 0)
        stop("no grouping variables supplied")

    outvars <- names(exprs)
    invars <- invars(exprs)

    levs <- getFactorCombinations(data, grps)

    on.exit(deleteIfTbl(data))

    # put grouping variable on to dataset
    output <- rxDataStep(unTbl(data), unTbl(tbl_xdf(data)), transformFunc=function(varlst) {
        varlst[[".group."]] <- .factor(varlst, .levs)
        varlst
    }, transformObjects=list(.levs=levs, .factor=makeGroupVar), transformVars=grps, overwrite=TRUE)

    smry <- smryRxSummary(output, ".group.", stats, exprs, rxArgs)
    
    # unsplit the grouping variable
    vars <- rebuildGroupVars(smry[1], grps, data)

    data.frame(vars, smry[-1], stringsAsFactors=FALSE)
}

