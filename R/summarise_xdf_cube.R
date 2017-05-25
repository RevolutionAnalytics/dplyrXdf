#' @include summarise_xdf.R
NULL


smryRxCube <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
        stop("rxCube method only for grouped xdf tbls")
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
    is_char <- varTypes(data, grps) == "character"
    if(any(is_char))
        data <- factorise_(data, .dots=grps[is_char])

    on.exit(deleteIfTbl(data))

    cl <- buildSmryFormulaRhs(data, grps,
        quote(rxCube(fm, data, means=means, useSparseCube=TRUE, removeZeroCounts=TRUE)))
    levs <- cl$levs
    cl$call[names(rxArgs)] <- rxArgs

    # single call to rxCube if only 1 summary statistic type, otherwise multiple calls
    if(length(unique(stats)) == 1)
    {
        fm <- formula(paste0("cbind(", paste0(invars, collapse=","), ") ~ ", cl$fmRhs))
        means <- stats[1] == "mean"
        df <- data.frame(eval(cl$call))
        df <- df[-ncol(df)]
    }
    else
    {
        df <- lapply(seq_along(stats), function(i) {
            means <- stats[i] == "mean"
            fm <- reformulate(cl$fmRhs, invars[[i]])
            cube <- data.frame(eval(cl$call))
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


buildSmryFormulaRhs <- function(data, grps, call)
{
    numeric_logical <- c("numeric", "integer", "logical", "Date", "POSIXct")
    gvarTypes <- varTypes(data, grps)

    # use fast rxCube/rxSummary if possible, otherwise construct grouping factor
    if(all(gvarTypes %in% c("factor", numeric_logical)))
    {
        # using F() assumes that numeric columns are integers; do a check on this
        if(any(gvarTypes %in% numeric_logical))
            verifyNumericsAreIntegers(data, grps)
        nRhs <- length(grps)
        call$transformFunc <- quote(function(varlst) {
            varlst[[".n."]] <- rep(1, .rxNumRows)
            varlst
        })
        call$transformVars <- quote(grps[1])
        rhs_vars <- ifelse(gvarTypes %in% numeric_logical,
            paste0("F(", grps, ")"), grps)
        fmRhs <- paste(rhs_vars, collapse=":")
        levs <- NULL
    }
    else
    {
        # smry_rxCube and smry_rxSummary methods should have converted all char columns to factor
        # this code block should never be needed, but leave it in as a safety check
        warning("unexpected non-factor, non-numeric grouping variable in summarise", call.=FALSE)
        levs <- getGroupLevels(data)
        # put grouping variable on to dataset in call to rxCube; faster than separate rxDataStep
        call$transformFunc <- quote(function(varlst) {
            varlst[[".group."]] <- .factor(varlst, .levs)
            varlst[[".n."]] <- rep(1, length(varlst[[1]]))
            varlst
        })
        call$transformObjects <- quote(list(.levs=levs, .factor=makeGroupVar))
        call$transformVars <- quote(grps)
        nRhs <- 1
        fmRhs <- ".group."
    }
    list(call=call, nRhs=nRhs, fmRhs=fmRhs, levs=levs)
}


setSmryClasses <- function(df, origdata, invars, outvars)
{
    types <- varTypes(origdata)
    smrytypes <- sapply(invars, function(x) if(x %in% names(origdata)) types[x] else "numeric")
    mapply(function(x, type) {
        newtype <- class(x)
        if(newtype != type &&
           !(newtype == "numeric" && type %in% c("logical", "integer")))  # don't convert results for unclassed ints back to int
            class(x) <- type
        x
    }, df, smrytypes, SIMPLIFY=FALSE)
}


verifyNumericsAreIntegers <- function(data, grps)
{
    data <- rxDataStep(data, varsToKeep=grps, numRows=1000)
    n <- 1
    while(n <= ncol(data))
    {
        x <- data[[n]]
        if(is.numeric(x) && any(x != floor(x)))
        {
            stop("non-integer values found for grouping variable ", names(data)[n],
                ": use factorise to get correct groups", call.=FALSE)
            break
        }
        n <- n + 1
    }
    NULL
}

