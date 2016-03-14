#' @include summarise_xdf.R
NULL


smry_PemaByGroup <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
        stop("PemaByGroup method only for grouped xdf tbls")
    outvars <- names(exprs)
    invars <- invars(exprs)

    levs <- get_grouplevels(data)
    # put grouping variable on to dataset
    data <- rxDataStep(data, tblSource(data), transformFunc=function(varlst) {
        varlst[[".group."]] <- .factor(varlst, .levs)
        varlst
    }, transformObjects=list(.levs=levs, .factor=make_groupvar), transformVars=grps, overwrite=TRUE)

    bg <- RevoPemaR::PemaByGroup()
    fnlist <- make_pema_fnlist(exprs, invars, outvars)
    smry <- pemaCompute(bg, data=data, groupByVar=".group.", computeVars=invars, fnList=fnlist)

    # keep desired columns
    nstat <- length(fnlist)
    fn_ptr <- attr(fnlist, "ptr")
    cols <- mapply(function(invar, stat, unique_invars) {
        i <- (which(invar == unique_invars) - 1) * nstat
        i + stat
    }, invars, fn_ptr, MoreArgs=list(unique_invars=unique(invars)))    

    smry <- smry[, c(ncol(smry), cols), drop=FALSE]
    names(smry)[-1] <- outvars

    # reconstruct grouping variables
    gvars <- rebuild_groupvars(smry[1], grps, data)

    rxDataStep(cbind(gvars, smry[-1], stringsAsFactors=FALSE), tblSource(data), overwrite=TRUE)
}


make_pema_fnlist <- function(exprs, invars, outvars)
{
    fnlist <- lapply(exprs, function(e) {
        if(identical(e, quote(n())))
            e[[1]] <- quote(length)
        l <- as.list(e)
        l[[1]] <- as.character(l[[1]])
        l[2] <- list(NULL)
        names(l)[1] <- "FUN"
        names(l)[2] <- ""
        l
    })
    names(fnlist) <- sapply(fnlist, function(f) as.character(f$FUN))

    # check for duplicated summary stats, retain pointers to correct stat
    # %in% requires doing comparisons in character mode
    dupes <- duplicated(fnlist)
    fnchar <- sapply(fnlist, function(f) deparse(as.call(f)))
    uniq <- fnlist[!dupes]
    uniqchar <- fnchar[!dupes]
    fn_ptr <- sapply(fnchar, function(f) which(uniqchar %in% f))

    attr(uniq, "ptr") <- fn_ptr
    uniq
}


