#' @include summarise_xdf.R
NULL


smryRxSummary <- function(data, grps=NULL, stats, exprs, rxArgs)
{
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
        quote(rxSummary(fm, data, summaryStats=uniqueStat, useSparseCube=TRUE, removeZeroCounts=TRUE)),
        rxArgs,
        anyN,
        names(data)[1], # rxSummary transform(n=1) fails if no other transforms present
        gvarTypes=gvarTypes)

    findTable <- function(s)
    {
        found <- FALSE
        for(table in tables)
        {
            lev <- levels(table[[1]])[1]
            found <- substr(lev, 1, nchar(s)) == s
            if(found) break
        }
        if(!found)
            stop("unable to find rxSummary output for ", s)
        table
    }

    rxSummaryStat <- function(x)
    {
        outStat <- c(mean="mean",
                     sum="sum",
                     sd="stddev",
                     var="stddev",
                     n="sum",
                     min="min",
                     max="max")
        outStat[x]
    }

    selectCol <- function(df, stat)
    {
        x <- switch(stat,
        mean=df$Mean, # note that cubes from rxSummary have a "Means" column; relying on partial evaluation here
        sum=df$Sum,
        sd=df$StdDev,
        var=df$StdDev ^ 2,
        n=df$Sum,
        min=df$Min,
        max=df$Max)
        # check if rxSummary screwed up
        if(is.null(x))
            stop("error in rxSummary")
        x
    }

    data <- unTbl(data) # workaround HDFS/tbl incompatibility

    if(length(grps) > 0)
    {
        fm <- reformulate(paste(invars, cl$fmRhs, sep=":"))
        uniqueStat <- unique(rxSummaryStat(stats))
        tables <- eval(cl$call)$categorical
        df <- lapply(seq_along(stats), function(i)
        {
            tab <- findTable(invars[[i]]) # have to search for correct table (!)
            x <- selectCol(tab, stats[i])
            cbind(tab[2:(cl$nRhs + 1)], x)
        })
        byvars <- names(df[[1]])[1:cl$nRhs]
        df <- Reduce(function(x, y) full_join(x, y, by=byvars), df)
        names(df)[-(1:cl$nRhs)] <- outvars

        # reconstruct grouping variables -- note this will keep char variables as factors
        gvars <- rebuildGroupVars(df[1:cl$nRhs], grps, data)
    }
    else
    {
        fm <- reformulate(invars)
        uniqueStat <- unique(rxSummaryStat(stats))
        smry <- eval(cl$call)$sDataFrame
        df <- lapply(seq_along(stats), function(i)
        {
            x <- smry[i,, drop=FALSE]
            selectCol(x, stats[i])
        })
        df <- data.frame(df)
        names(df) <- outvars
    }

    # reassign classes to outputs (for Date and POSIXct objects; work around glitch in rxCube, rxSummary)
    df <- setSmryClasses(df[outvars], data, invars, outvars)

    on.exit(deleteIfTbl(data))
    if(length(grps) > 0)
        data.frame(gvars, df, stringsAsFactors=FALSE)
    else data.frame(df)
}

