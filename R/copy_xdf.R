# get level sets of factors to merge on
# return _changes_ to variables needed to ensure merge is successful
setLevelsEqual <- function(x, y, by)
{
    factorList <- function(data, vars)
    {
        varInfo <- rxGetVarInfo(data, varsToKeep=vars)
        fac <- which(sapply(varInfo, "[[", "varType") == "factor")
        if(length(fac) == 0)
            return(NULL)
        sapply(varInfo[fac], "[[", "levels", simplify=FALSE)
    }

    factorsToChange <- function(oneFactor)
    {
        out <- sapply(names(allFactors), function(f) {
            oneFac <- oneFactor[[f]]
            allFac <- allFactors[[f]]
            if(length(oneFac) != length(allFac) || any(oneFac != allFac))  # could also use identical()
                allFac
            else NULL
        }, simplify=FALSE)
        out[!sapply(out, is.null)]
    }

    xFactors <- factorList(x, by$x)
    yFactors <- factorList(y, by$y)
    allFactors <- sapply(base::union(names(xFactors), names(yFactors)), function(f)
        sort(base::union(xFactors[[f]], yFactors[[f]])),
        simplify=FALSE)
    list(x=factorsToChange(xFactors), y=factorsToChange(yFactors))
}


asXdfOrDf <- function(x)
{
    if(inherits(x, c("data.frame", "RxXdfData")))
        x
    else if(inherits(x, "RxFileData"))
        tbl(x, stringsAsFactors=FALSE)
    else stop("not a local data source format", call.=FALSE)
}


alignInputs <- function(x, y, by)
{
    origxy <- list(x=if(is.data.frame(x)) NULL else x,
                   y=if(is.data.frame(y)) NULL else y)

    # data must be in xdf or data frame format, import if not
    x <- asXdfOrDf(x)
    y <- asXdfOrDf(y)

    # rxMerge requires common varnames; transform x if necessary
    if(!identical(by$x, by$y))
    {
        # if transformation will overwrite existing variables, drop them to avoid type clashes
        drop <- base::intersect(by$y, names(x))
        if(length(drop) == 0)
            drop <- NULL
        xby <- setNames(by$x, by$y)
        x <- mutate_(x, .dots=xby, .rxArgs=list(varsToDrop=drop))
    }
    
    # check compatibility of factor types
    combinedFactors <- setLevelsEqual(x, y, by)
    if(length(combinedFactors$x) > 0)
        x <- factorise_(x, .dots=combinedFactors$x)
    if(length(combinedFactors$y) > 0)
    {
        # make sure not to delete original y by accident after factoring
        # origy is df -> y is df -> don't delete
        # origy is tbl -> y is tbl -> don't delete
        # origy is xdf -> y is xdf -> don't delete
        # origy is txt -> y is tbl -> delete
        if(!inherits(origxy$y, "RxXdfData") && inherits(y, "tbl_xdf"))
            y <- as(y, "RxXdfData")
        y <- factorise_(y, .dots=combinedFactors$y)
    }
    list(x=x, y=y, orig=origxy)
}

