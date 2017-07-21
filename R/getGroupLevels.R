getFactorLevels <- function(data, vars=group_vars(data))
{
    if(length(vars) == 0)
        return(NULL)

    levs <- if(isHdfs(data))
    {
        # workaround crazy rxExecBy issue
        vars <- unname(vars)

        # keep only the variables we need
        tmpSrc <- RxXdfData(data@file, fileSystem=data@fileSystem, createCompositeSet=data@createCompositeSet,
            varsToKeep=vars)
        message("Scanning data to get levels")

        # use rxExecBy on HDFS: only for the keys, not the data
        cc <- rxGetComputeContext()
        on.exit(rxSetComputeContext(cc))
        keys <- lapply(rxExecBy(tmpSrc, vars, function(keys, data) 0), "[[", "keys")

        lapply(seq_along(vars), function(i)
            unique(sapply(keys, function(k) k[[i]])))
    }
    else
    {
        levs <- sapply(vars, function(x) logical(0))
        rxDataStep(data, varsToKeep=vars, transformFunc=function(varlst)
        {
            for(i in seq_along(names(varlst)))
                .levs[[i]] <<- unique(c(.levs[[i]], varlst[[i]]))
            NULL
        }, transformObjects=list(.levs=levs), returnTransformObjects=TRUE)$.levs
    }

    names(levs) <- vars
    levs
}


getFactorCombinations <- function(data, vars=group_vars(data))
{
    stopIfHdfs("getFactorCombinations not supported on HDFS") # should never trip this

    if(length(vars) == 0)
        return(NULL)

    levdf <- as.data.frame(sapply(vars, function(xi) logical(0), simplify=FALSE))

    # read grouping variables by block, return unique row combinations
    levs <- rxDataStep(data, varsToKeep=vars, transformFunc=function(varlst)
    {
        .levdf <<- dplyr::distinct(rbind(.levdf, as.data.frame(varlst)))
        NULL
    }, transformObjects=list(.levdf=levdf), transformPackages="dplyr", returnTransformObjects=TRUE)[[1]]

    levs <- do.call(paste, c(levs, sep="_&&_"))
    levs
}


makeGroupVar <- function(gvars, levs)
{
    factor(do.call(paste, c(gvars, sep="_&&_")), levels=levs)
}
