smryRxCube <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
        stop("rxCube method only for grouped xdf tbls")
    outvars <- names(exprs)
    invars <- invars(exprs)

    # workaround for glitch in observation count with rxSummary, rxCube; also makes counting easier
    anyN <- any(stats == "n")
    if(anyN)
    {
        bad <- which(stats == "n")
        stats[bad] <- "sum"
        invars[bad] <- ".n."
    }
    stopifnot(all(nchar(invars) > 0))

    # convert non-factor character cols into factors
    gvarTypes <- varTypes(data, grps)
    isChar <- gvarTypes == "character"
    if(any(isChar))
    {
        data <- factorise(data, !!!rlang::syms(grps[isChar]))
        gvarTypes[isChar] <- "factor"
    }

    cl <- buildSmryFormulaRhs(data, grps,
        quote(rxCube(fm, data, means=means, useSparseCube=TRUE, removeZeroCounts=TRUE, returnDataFrame=TRUE)),
        rxArgs, anyN, gvarTypes=gvarTypes)

    oldData <- data
    on.exit(deleteIfTbl(oldData))
    data <- unTbl(data) # workaround HDFS/tbl incompatibility

    # single call to rxCube if only 1 summary statistic type, otherwise multiple calls
    if(length(unique(stats)) == 1)
    {
        fm <- formula(paste0("cbind(", paste0(invars, collapse=","), ") ~ ", cl$fmRhs))
        means <- stats[1] == "mean"
        df <- eval(cl$call)
        class(df) <- "data.frame"
        df <- df[-ncol(df)]
    }
    else
    {
        df <- lapply(seq_along(stats), function(i) {
            means <- stats[i] == "mean"
            fm <- reformulate(cl$fmRhs, invars[[i]])
            cube <- eval(cl$call)
            class(cube) <- "data.frame"
            cube[-ncol(cube)]
        })
        byvars <- names(df[[1]])[1:cl$nRhs]
        df <- Reduce(function(x, y) full_join(x, y, by=byvars), df)
    }
    names(df)[-(1:cl$nRhs)] <- outvars

    # reconstruct grouping variables -- note this will keep char variables as factors
    gvars <- rebuildGroupVars(df[1:cl$nRhs], grps, data)
    
    # reassign classes to outputs (for Date and POSIXct objects; work around glitch in rxCube, rxSummary)
    df <- setSmryClasses(df[outvars], data, invars, outvars)

    data.frame(gvars, df, stringsAsFactors=FALSE)
}

