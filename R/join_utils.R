# align types of by-variables
# return _changes_ to variables needed to ensure merge is successful
# - check all non-factor types in x and y
# - if var type not the same in both, coerce as appropriate (logical < numeric < complex < character)
alignVarTypes <- function(x, y, by)
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


alignInputs <- function(x, y, by, yOrig)
{

    asXdfOrDf <- function(data)
    {
        if(inherits(data, c("data.frame", "RxXdfData")))
            data
        else if(inherits(data, "RxFileData"))
            tbl(data, stringsAsFactors=FALSE)
        else stop("not a local data source format", call.=FALSE)
    }

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
    combinedFactors <- setLevelsEqual(x, y, by$y)
    if(length(combinedFactors$x) > 0)
        x <- factorise_(x, .dots=combinedFactors$x)
    if(length(combinedFactors$y) > 0)
    {
        # make sure not to delete original y by accident after factoring
        if(!is.null(yOrig) && getTblFile(y) == yOrig)
            y <- as(y, "RxXdfData")
        y <- factorise_(y, .dots=combinedFactors$y)
    }
    list(x=x, y=y, yOrig=yOrig)
}


# copied from dplyr:::common_by, dplyr:::`%||%`
dplyr_common_by <- function (by = NULL, x, y) 
{
    if (is.list(by)) 
        return(by)
    if (!is.null(by)) {
        x <- if(is.null(names(by))) by else names(by)
        #x <- names(by) %||% by
        y <- unname(by)
        x[x == ""] <- y[x == ""]
        return(list(x = x, y = y))
    }
    by <- intersect(tbl_vars(x), tbl_vars(y))
    if (length(by) == 0) {
        stop("No common variables. Please specify `by` param.", call. = FALSE)
    }
    message("Joining by: ", capture.output(dput(by)))
    list(x = by, y = by)
}
