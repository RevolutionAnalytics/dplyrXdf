#' @include summarise_xdf.R
#' @include summarise_xdf_cube.R
NULL


smryRxSummary <- function(data, ...)
UseMethod("smryRxSummary")


smryRxSummary.grouped_tbl_xdf <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    outvars <- names(exprs)
    invars <- invars(exprs)

    # workaround for glitch in observation count with rxSummary, rxCube; also makes counting easier
    if(any(stats == "n"))
    {
        bad <- which(stats == "n")
        stats[bad] <- "sum"
        invars[bad] <- ".n."
    }
    stopifnot(all(nchar(invars) > 0))

    # convert non-factor character cols into factors
    isChar <- varTypes(data, grps) == "character"
    if(any(isChar))
        data <- factorise_(data, .dots=grps[isChar])

    oldData <- data
    if(hasTblFile(data))
        on.exit(deleteTbl(oldData))

    cl <- build_smry_formula_rhs(data, grps,
        quote(rxSummary(fm, data, summaryStats=uniqueStat, useSparseCube=TRUE, removeZeroCounts=TRUE)))
    cl$call[names(rxArgs)] <- rxArgs

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

    levs <- cl$levs
    fm <- reformulate(paste(invars, cl$fm_rhs, sep=":"))
    uniqueStat <- unique(rxSummaryStat(stats))
    tables <- eval(cl$call)$categorical
    df <- lapply(seq_along(stats), function(i) {
        tab <- findTable(invars[[i]])  # have to search for correct table (!)
        x <- selectCol(tab, stats[i])
        cbind(tab[2:(cl$n_rhs + 1)], x)
    })
    byvars <- names(df[[1]])[1:cl$n_rhs]
    df <- Reduce(function(x, y) full_join(x, y, by=byvars), df)
    names(df)[-(1:cl$n_rhs)] <- outvars

    # reconstruct grouping variables -- note this will keep char variables as factors
    gvars <- rebuildGroupVars(df[1:cl$n_rhs], grps, data)

    # reassign classes to outputs (for Date and POSIXct objects; work around glitch in rxCube, rxSummary)
    df <- setSmryClasses(df[outvars], data, invars, outvars)

    data.frame(gvars, df, stringsAsFactors=FALSE)
}


# define method for RxFileData because this is used by rxSplit method with raw xdf as input, and with non-xdf data sources
smryRxSummary.RxFileData <- function(data, grps=NULL, stats, exprs, rxArgs, dfOut=FALSE)
{
    outvars <- names(exprs)
    invars <- invars(exprs)

    oldData <- data
    if(hasTblFile(data))
        on.exit(deleteTbl(oldData))

    cl <- quote(rxSummary(fm, data, summaryStats=uniqueStat, useSparseCube=TRUE, removeZeroCounts=TRUE, transformFunc=function(varlst) {
        varlst[[".n."]] <- rep(1, length(varlst[[1]]))
        varlst
    }, transformVars=names(data)[1]))
    cl[names(rxArgs)] <- rxArgs

    # workaround for glitch in observation count with rxSummary, rxCube; also makes counting easier
    if(any(stats == "n"))
    {
        bad <- which(stats == "n")
        stats[bad] <- "sum"
        invars[bad] <- ".n."
        cl$transforms <- if(is.null(cl$transforms))
            list(quote(.n.=rep(1, .rxNumRows)))
        else
        {
            ntrans <- length(cl$transforms) + 1
            cl$transforms[[ntran]] <- quote(rep(1, .rxNumRows))
            names(cl$transform)[ntrans] <- ".n."
        }
    }
    stopifnot(all(nchar(invars) > 0))

    fm <- reformulate(invars)
    uniqueStat <- unique(rxSummaryStat(stats))
    smry <- eval(cl)$sDataFrame
    df <- lapply(seq_along(stats), function(i) {
        x <- smry[i, , drop=FALSE]
        selectCol(x, stats[i])
    })
    df <- data.frame(df)
    names(df) <- outvars

    # reassign classes to outputs (for Date and POSIXct objects; work around glitch in rxCube, rxSummary)
    df <- setSmryClasses(df[outvars], data, invars, outvars)

    if(dfOut)  # output as data frame
        df
    else rxDataStep(as.data.frame(df), tbl(newTbl(data), hasTblFile=TRUE), overwrite=TRUE)
}


rxSummaryStat <- function(x)
{
    outStat <- c(mean = "mean",
                 sum = "sum",
                 sd = "stddev",
                 var = "stddev",
                 n = "sum",
                 min = "min",
                 max = "max")
    outStat[x]
}


selectCol <- function(df, stat)
{
    x <- switch(stat,
        mean = df$Mean,  # note that cubes from rxSummary have a "Means" column; relying on partial evaluation here
        sum = df$Sum,
        sd = df$StdDev,
        var = df$StdDev^2,
        n = df$Sum,
        min = df$Min,
        max = df$Max)
    # check if rxSummary screwed up
    if(is.null(x))
        stop("error in rxSummary")
    x
}

