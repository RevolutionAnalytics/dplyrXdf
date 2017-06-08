getGroupLevels <- function(data, gvars=group_vars(data))
{
    stopIfHdfs("getGroupLevels not supported on HDFS") # should never trip this

    if(length(gvars) == 0)
        return(NULL)

    levdf <- as.data.frame(sapply(gvars, function(xi) logical(0), simplify=FALSE))

    # read grouping variables by block, return unique row combinations
    levs <- rxDataStep(data, varsToKeep=gvars, transformFunc=function(varlst)
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
