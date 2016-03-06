#' @export
same_src.RxXdfData <- function(x, y)
{
    # not technically the same source, but rxMerge can handle data frames natively
    inherits(y, c("RxXdfData", "data.frame"))
}


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


xdf_copy <- function(x, y, copy=FALSE, by)
{
    # data must be in xdf or data frame format, import if not
    if(!same_src(x, y))
    {
        if(inherits(y, c("RxTextData", "RxSasData", "RxSpssData")))
            y <- tbl(y, stringsAsFactors=FALSE)
        else if(inherits(y, "tbl_sql"))
        {
            # only copy data from dplyr SQL data source if copy==TRUE
            if(copy)
                y <- collect(y)
            else stop("merging with dplyr SQL data source requires copying the data into memory; set copy=TRUE to allow this", call.=FALSE)
        }
        else stop("data must be in an xdf or data frame", call.=FALSE)
    }

    # rxMerge requires common varnames; transform if necessary
    if(!identical(by$x, by$y))
    {
        # if transformation will overwrite existing variables, drop them first to avoid type clashes
        if(any(by$x %in% tbl_vars(y)))
        {
            drop <- sapply(by$x, function(x) ~NULL)
            y <- mutate_(y, .dots=drop)
        }
        by <- setNames(lapply(by$y, as.name), by$x)
        y <- mutate_(y, .dots=by)
    }
    
    # check compatibility of factor types
    # TODO: combine this with mutate step above, extend checking to more than just factors
    combinedFactors <- setLevelsEqual(x, y, by)
    if(length(combinedFactors$x) > 0)
        x <- do.call(factorise, c(list(x), combinedFactors$x))
    if(length(combinedFactors$y) > 0)
        y <- do.call(factorise, c(list(y), combinedFactors$y))

    list(x=x, y=y)
}

