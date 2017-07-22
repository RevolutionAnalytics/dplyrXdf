buildSmryFormulaRhs <- function(data, grps, call, rxArgs, addN=FALSE, proxyVar=FALSE, gvarTypes)
{
    if(!is.null(rxArgs))
        call <- rlang::lang_modify(call, rlang::splice(rxArgs))

    if(addN)
    {
        # rxSummary transform(n=1) fails if no other transforms present
        if(is.character(proxyVar))
        {
            if(!is.null(call$transforms))
                call$transforms$.n. <- substitute(rep(1, length(.x)), list(.x=as.name(proxyVar)))
            else call$transforms <- substitute(list(.n.=rep(1, length(.x))), list(.x=as.name(proxyVar)))
        }
        else
        {
            if(!is.null(call$transforms))
                call$transforms$.n. <- 1
            else call$transforms <- quote(list(.n.=1))
        }
    }

    nRhs <- length(grps)
    fmRhs <- if(nRhs > 0)
    {
        numeric_logical <- c("numeric", "integer", "logical", "Date", "POSIXct")

        # smry_rxCube and smry_rxSummary methods should have converted all char columns to factor
        if(!all(gvarTypes %in% c("factor", numeric_logical)))
            stop("unexpected non-factor, non-numeric grouping variable in summarise", call.=FALSE)

        # using F() assumes that numeric columns are integers; do a check on this
        # don't check on HDFS for performance reasons
        if(any(gvarTypes %in% numeric_logical) && !in_hdfs(data))
            verifyNumericsAreIntegers(data, grps)

        rhsVars <- ifelse(gvarTypes %in% numeric_logical, paste0("F(", grps, ")"), grps)
        paste(rhsVars, collapse=":")
    }
    else character(0)

    list(call=call, nRhs=nRhs, fmRhs=fmRhs)
}


setSmryClasses <- function(df, origdata, invars, outvars)
{
    types <- varTypes(origdata)
    smrytypes <- sapply(invars, function(x) if(x %in% names(origdata)) types[x] else "numeric")
    mapply(function(x, type)
    {
        newtype <- class(x)
        if(newtype != type &&
            !(newtype == "numeric" && type %in% c("logical", "integer"))) # don't convert results for unclassed ints back to int
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
