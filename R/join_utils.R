# align by-variables for joining
# - check that varnames match (required by rxMerge)
# - check vartypes match
# - check factor levels match
# - by default, change x to match y; exception is when factors are involved (may need to change y's levels)
# return transformFuncs suitable for passing to mutate_
# - inline transforms don't work (will leave old variable behind)
# - changing variable type in-place also doesn't work (need to create new variable, drop old variable)
alignVars <- function(x, y, by, yOrig)
{
    makeTransformFunc <- function(exprlst, returnFunction=TRUE)
    {
        nulls <- sapply(exprlst, is.null)
        if(all(nulls))
            return(NULL)
        exprlst <- c(exprlst, lapply(names(exprlst)[!nulls], function(e) {
            parse(text=sprintf("%s <- NULL", e))
        }))
        exprlst <- do.call(c, exprlst)                  # convert list of expressions into single expression object
        exprblock <- as.call(c(as.name("{"), exprlst))  # from ?call
        if(returnFunction)
        {
            xFunc <- function(varlst) {}
            body(xFunc) <- call("within", quote(data.frame(varlst, stringsAsFactors=FALSE)), exprblock)
            xFunc
        }
        else call("within", quote(data.frame(y)), exprblock)
    }

    getTransformVars <- function(exprlst)
    {
        nulls <- sapply(exprlst, is.null)
        names(exprlst)[!nulls]
    }

    unFactor <- function(var, type)
    {
        changeExpr <- parse(text=sprintf("%s__new__ <- as.character(%s)", var, var))
        if(type %in% c("logical", "integer", "numeric", "complex"))
            changeExpr <- c(changeExpr, parse(text=sprintf("%s__new__ <- as.%s(%s__new__)", var, type, var)))
        else if(type != "character")
            stop("don't know how to convert type: ", type)
        changeExpr
    }

    reFactor <- function(var)
    {
        # ensure level sets of x and y are the same: rxFactors very picky
        xlevs <- varLevels(x)[[var]]
        ylevs <- varLevels(y)[[var]]
        if(!identical(xlevs, ylevs))
        {
            levs <- sort(union(xlevs, ylevs))
            levs <- paste(deparse(levs), collapse="")
            parse(text=sprintf("%s__new__ <- factor(%s, levels=%s)", var, var, levs))
        }
        else NULL
    }

    xVars <- names(x)
    yVars <- names(y)
    xTypes <- varTypes(x, by)
    yTypes <- varTypes(y, by)

    xChanges <- sapply(by, function(i) {
        xt <- xTypes[i]
        yt <- yTypes[i]
        changeExpr <- NULL

        if(xt != "factor" && yt != "factor")  # (relatively) clean bit: no factors involved
        {
            if(xt != yt)
            {
                if(yt %in% c("logical", "integer", "numeric", "complex", "character"))
                    changeExpr <- parse(text=sprintf("%s__new__ <- as.%s(%s)", i, yt, i))
                else stop("don't know how to convert type: ", yt)
            }
        }
        else if(xt == "factor" && yt != "factor")  # un-factor x (path of least resistance)
            changeExpr <- unFactor(i, yt)
        else if(xt == "factor" && yt == "factor")  # combine x and y levels
            changeExpr <- reFactor(i)

        changeExpr
    }, simplify=FALSE)

    stopifnot(inherits(x, "RxXdfData"))  # x should always inherit from RxXdfData
    xFunc <- makeTransformFunc(xChanges, inherits(x, "RxXdfData"))
    xVars <- getTransformVars(xChanges)

    yChanges <- sapply(by, function(i) {
        xt <- xTypes[i]
        yt <- yTypes[i]
        changeExpr <- NULL

        # only change y if need to un-factor or relevel a factor
        if(xt != "factor" && yt == "factor")  # un-factor y (path of least resistance)
            changeExpr <- unFactor(i, xt)
        else if(xt == "factor" && yt == "factor")  # combine x and y levels
            changeExpr <- reFactor(i)

        changeExpr
    }, simplify=FALSE)

    yFunc <- makeTransformFunc(yChanges, inherits(y, "RxXdfData"))
    yVars <- getTransformVars(yChanges)

    list(xFunc=xFunc, xVars=xVars, yFunc=yFunc, yVars=yVars)
}


alignInputs <- function(x, y, by, yOrig)
{

    asXdfOrDf <- function(data)
    {
        if(inherits(data, c("data.frame", "RxXdfData")))
            data
        else if(inherits(data, "RxFileData"))
            as(data, "tbl_xdf")
        else stop("not a local data source format", call.=FALSE)
    }

    # data must be in xdf or data frame format, import if not
    x <- asXdfOrDf(x)
    y <- asXdfOrDf(y)

    # rxMerge requires identical by-variable names; rename xvars if necessary
    if(!identical(by$x, by$y))
    {
        xby <- setNames(by$x, by$y)

        # if renaming clashes with other variables, rename them as well (this is not recursive)
        # need separate rename call because sequential renaming (a -> b -> c) bugged
        existing <- base::intersect(by$y, names(x))
        if(length(existing) > 0)
        {
            names(existing) <- paste0(names(existing), ".x")
            x <- rename(x, !!!existing)
        }
        x <- rename(x, !!!xby)
    }
    
    # align by-variable types and factor levels
    align <- alignVars(x, y, by$y)
    if(!is.null(align$xFunc))
    {
        xRename <- paste0(align$xVars, "__new__")
        names(xRename) <- align$xVars
        x <- mutate(x, .rxArgs=list(transformFunc=align$xFunc, transformVars=align$xVars)) %>%
            rename(!!!xRename)
    }
    if(!is.null(align$yFunc))
    {
        yRename <- paste0(align$yVars, "__new__")
        names(yRename) <- align$yVars
        if(is.null(yOrig))
            y <- eval(align$yFunc) %>% rename(!!!yRename)
        else
        {
            # make sure not to delete original y by accident after factoring
            if(!is.null(yOrig) && getTblFile(y) == yOrig)
                y <- as(y, "RxXdfData")
            #y <- rxDataStep(y, tbl_xdf(y), transformFunc=align$yFunc, transformVars=align$yVars)
            #names(y)[names(y) == yRename] <- names(yRename)
            #print(names(y))
            y <- mutate(y, .rxArgs=list(transformFunc=align$yFunc, transformVars=align$yVars)) %>%
                rename(!!!yRename)
            #y <- mutate(y, a__new__=as.character(a), a=NULL)
            #y <- rename(y, a=a__new__)
        }
    }
    list(x=x, y=y)
}


# copied from dplyr:::common_by, dplyr:::`%||%`
commonBy <- function (by = NULL, x, y) 
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


mergeBase <- function(x, y, by=NULL, copy=FALSE, type, .outFile=tbl_xdf(x), .rxArgs, suffix=c(".x", ".y"))
{
    stopIfHdfs(x, "merging not supported on HDFS")
    stopIfHdfs(y, "merging not supported on HDFS")

    # copy not used by dplyrXdf at the moment
    if(copy)
        warning("copy argument not currently used")

    grps <- group_vars(x)
    yOrig <- getTblFile(y)
    newxy <- alignInputs(x, y, by, yOrig)
    x <- newxy$x
    y <- newxy$y

    # cleanup on exit is asymmetric wrt x, y
    on.exit(
    {
        deleteIfTbl(x)
        # make sure not to delete original y by accident after factoring
        if(!is.data.frame(y) && getTblFile(y) != yOrig)
            deleteTbl(y)
    })

    # sigh
    if(all(substr(suffix, 1, 1) == "."))
        suffix <- substr(suffix, 2, nchar(suffix))

    arglst <- list(x, y, matchVars=by$y, type=type, duplicateVarExt=suffix)
    arglst <- doExtraArgs(arglst, x, .rxArgs, .outFile)
    arglst$rowsPerRead <- NULL # not used by rxMerge

    output <- rlang::invoke("rxMerge", arglst, .env=parent.frame(), .bury=NULL)
    simpleRegroup(output, grps)
}
