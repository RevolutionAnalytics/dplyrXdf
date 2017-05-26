#' @include summarise_xdf.R
#' @include summarise_xdf_summary.R
NULL


smryRxSummary2 <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    # should always have groups
    if(is.null(grps))
        stop("no grouping variables supplied", call.=FALSE)

    outvars <- names(exprs)
    invars <- invars(exprs)

    levs <- getGroupLevels(data)

    on.exit(deleteIfTbl(data))

    # put grouping variable on to dataset
    output <- rxDataStep(data, tbl_xdf(data), transformFunc=function(varlst) {
        varlst[[".group."]] <- .factor(varlst, .levs)
        varlst
    }, transformObjects=list(.levs=levs, .factor=makeGroupVar), transformVars=grps, overwrite=TRUE)
    output <- as(output, "grouped_tbl_xdf")
    output@groups <- grps
    output@hasTblFile <- TRUE

    smry <- smryRxSummary(output, ".group.", stats, exprs, rxArgs)
    
    # unsplit the grouping variable
    vars <- rebuildGroupVars(smry[1], grps, data)

    data.frame(vars, smry[-1], stringsAsFactors=FALSE)
}

