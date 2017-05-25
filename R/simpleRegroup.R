simpleRegroup <- function(x, grps=character(0))
{
    # if x is raw xdf, don't save grouping info
    if(length(grps) > 0 && inherits(x, c("tbl_xdf", "data.frame")))
        group_by_at(x, grps)
    else x
}
