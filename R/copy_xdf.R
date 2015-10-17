#' @export
same_src.RxXdfData <- function(x, y)
{
    # not technically the same source, but rxMerge can handle data frames natively
    inherits(y, c("RxXdfData", "data.frame"))
}


xdf_copy <- function(x, y, copy=FALSE, by)
{
    if(!same_src(x, y))
    {
        if(copy)
        {
            if(inherits(y, c("RxTextData", "RxSasData", "RxSpssData")))
                y <- tbl(y)
            else stop("don't know how to convert y to Xdf", call.=FALSE)
        }
        else stop("x and y don't share the same src. Set copy = TRUE to copy y into ", 
            "x's source (this may be time consuming).", call.=FALSE)
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
    xfactors <- names(which(varTypes(x, by$x) == "factor"))
    yfactors <- names(which(varTypes(y, by$x) == "factor"))
    if(!all(xfactors %in% yfactors))
        y <- factorise_(y, .dots=xfactors)
    if(!all(yfactors %in% xfactors))
        x <- factorise_(x, .dots=yfactors)
    list(x=x, y=y)
}

