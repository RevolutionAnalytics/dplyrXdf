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

    # only read metadata once per file: slow on Spark
    xVars <- names(by$xTypes)
    yVars <- names(by$yTypes)
    xTypes <- by$xTypes[by$y]
    yTypes <- by$yTypes[by$y]

    by <- by$y
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

        # modify saved metadata to match any renamed vars
        whichXBy <- which(names(by$xTypes) %in% by$x)
        names(by$xTypes)[whichXBy] <- names(xby)
    }

    # align by-variable types and factor levels
    align <- alignVars(x, y, by)
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
            if(!is.null(yOrig) && y@file == yOrig)
                y <- as(y, "RxXdfData")
            y <- mutate(y, .rxArgs=list(transformFunc=align$yFunc, transformVars=align$yVars)) %>%
                rename(!!!yRename)
        }
    }
    list(x=x, y=y)
}


# copied from dplyr:::common_by, dplyr:::`%||%`
commonBy <- function (by = NULL, x, y) 
{
    # save metadata: important on Spark to minimise retrieving this
    xTypes <- tbl_types(x)
    yTypes <- tbl_types(y)
    xVars <- names(xTypes)
    yVars <- names(yTypes)

    if(is.list(by))
    {
        by <- c(by, xTypes=xTypes, yTypes=yTypes)
        return(by)
    }

    if(!is.null(by))
    {
        x <- if(is.null(names(by))) by else names(by)
        y <- unname(by)
        x[x == ""] <- y[x == ""]
        return(list(x=x, y=y, xTypes=xTypes, yTypes=yTypes))
    }

    by <- base::intersect(xVars, yVars)
    if(length(by) == 0)
    {
        stop("No common variables. Please specify `by` param.", call.=FALSE)
    }
    message("Joining by: ", capture.output(dput(by)))
    list(x=by, y=by, xTypes=xTypes, yTypes=yTypes)
}


mergeBase <- function(x, y, by=NULL, copy=FALSE, type, .outFile=tbl_xdf(x), .rxArgs, suffix=c(".x", ".y"))
{
    # copy not used by dplyrXdf at the moment
    if(copy)
    {
        warning("copy argument not yet implemented")
    }

    mergeFsCheck(x, y, .outFile, copy)

    grps <- group_vars(x)
    yOrig <- if(inherits(y, "RxFileData")) y@file else NULL
    newxy <- alignInputs(x, y, by, yOrig)
    x <- newxy$x
    y <- newxy$y

    # cleanup on exit is asymmetric wrt x, y
    on.exit(
    {
        deleteIfTbl(x)
        # make sure not to delete original y by accident after factoring
        if(!is.null(yOrig) && y@file != yOrig)
            deleteIfTbl(y)
    })

    # sigh
    if(all(substr(suffix, 1, 1) == "."))
        suffix <- substr(suffix, 2, nchar(suffix))

    arglst <- list(x, y, matchVars=by$y, type=type, duplicateVarExt=suffix)
    arglst <- doExtraArgs(arglst, x, .rxArgs, .outFile)
    arglst$rowsPerRead <- NULL # not used by rxMerge

    inputHd <- in_hdfs(x)

    # rxMerge won't create a data frame directly from HDFS input; do it the hard way
    outputDf <- is.null(.outFile)
    if(inputHd && outputDf)
        arglst$outFile <- tbl_xdf(x)

    # rxMerge writes to wrong location in Spark if given relative output path
    if(inputHd && substr(arglst$outFile@file, 1, 1) != "/")
        arglst$outFile <- modifyXdf(arglst$outFile, file=normalizeHdfsPath(arglst$outFile@file))

    output <- callRx("rxMerge", arglst)

    if(inputHd && outputDf)
    {
        outTbl <- arglst$outFile
        on.exit(deleteIfTbl(outTbl))
        output <- as.data.frame(output)
    }

    simpleRegroup(output, grps)
}


mergeFsCheck <- function(x, y, outFile, copy)
{
    if(in_hdfs(x) != in_hdfs(y))
    {
        if(!copy)
            stop("x and y must both be in the same filesystem; use copy=TRUE to join", call.=FALSE)
        else y <- copy_to(RxFileSystem(x), y)
    }

    # nothing in HDFS: return early
    if(!in_hdfs(x) && !in_hdfs(y))
        return(list(x, y))

    xSupported <- inherits(x, c("RxSparkData", "RxXdfData"))
    ySupported <- inherits(y, c("RxSparkData", "RxXdfData"))
    if(!xSupported || !ySupported)
        stop("unsupported HDFS file type for merge", call.=FALSE)

    cc <- rxGetComputeContext()
    if(!inherits(cc, "RxSpark"))
        stop("files in HDFS can only be merged in the Spark compute context", call.=FALSE)

    list(x, y)
}

