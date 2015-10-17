#' @include summarise_xdf.R
#' @include summarise_xdf_summary.R
NULL


smry_rxSummary2 <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
        return(smry_rxSummary(data, NULL, stats, exprs, rxArgs))

    outvars <- names(exprs)
    invars <- invars(exprs)

    levs <- get_grouplevels(data)
    # put grouping variable on to dataset
    data <- rxDataStep(data, tblFile(data), transformFunc=function(varlst) {
        varlst[[".group."]] <- .factor(varlst, .levs)
        varlst
    }, transformObjects=list(.levs=levs, .factor=make_groupvar), transformVars=grps, overwrite=TRUE)
    data <- as(data, "grouped_tbl_xdf")
    data@groups <- grps
    data@hasTblFile <- TRUE

    smry <- smry_rxSummary(data, ".group.", stats, exprs, rxArgs)
    
    # unsplit the grouping variable
    vars <- rebuild_groupvars(smry[1], grps, data)

    data.frame(vars, smry[-1], stringsAsFactors=FALSE)
}

