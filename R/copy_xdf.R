#' @export
same_src.RxXdfData <- function(x, y)
{
    # not technically the same source, but rxMerge can handle data frames natively
    inherits(y, c("RxXdfData", "data.frame"))
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
    # TODO: this doesn't check factor _levels_, fix so it does
    xfactors <- names(which(varTypes(x, by$x) == "factor"))
    yfactors <- names(which(varTypes(y, by$x) == "factor"))
    if(!all(xfactors %in% yfactors))
        y <- factorise_(y, .dots=xfactors)
    if(!all(yfactors %in% xfactors))
        x <- factorise_(x, .dots=yfactors)
    list(x=x, y=y)
}

