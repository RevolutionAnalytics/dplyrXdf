split_groups <- function(data, outXdf=data)
{
    if(is.character(outXdf))
        stop("must supply output data source to split_groups")
    grps <- groups(data)

    # rxSplit not supported on HDFS -- fake it with multiple rxDataSteps
    # this will be very slow with large no. of factor levels
    if(inherits(rxGetFileSystem(data), "RxHdfsFileSystem"))
    {
        levs <- get_grouplevels(data)
        data <- rxDataStep(data, outXdf, transformFunc=function(varlst) {
            varlst[[".group."]] <- .factor(varlst, .levs)
            varlst
        }, transformObjects=list(.levs=levs, .factor=make_groupvar), transformVars=grps, overwrite=TRUE)

        outFile <- rxXdfFileName(outXdf)
        lst <- sapply(levs, function(l) {
            thisXdf <- outXdf
            thisXdf@file <- paste(sub(".xdf$", "", outFile), "_lev_", l, ".xdf", sep="")
            cl <- substitute(rxDataStep(data, thisXdf, rowsToKeep=.group. == .l, overwrite=TRUE),
                list(.l=l))
            tbl(eval(cl), hasTblFile=TRUE)
        }, simplify=FALSE)
        return(lst)
    }
    else
    {
        fname <- tools::file_path_sans_ext(rxXdfFileName(outXdf))
        fname <- file.path(dxGetWorkDir(), basename(fname))

        # if files exist that could interfere with splitting output, delete them
        # should never be necessary because base filename is a randomly generated tempfile
        # and each split should be followed by a combine
        delete_split_outputs(fname, grps)

        # mimic behaviour of rxSplit: rxDataStep that splits each chunk, calls rxDataStep on each split
        lst <- rxDataStep(data, transformFunc=function(varlst) {
                datlst <- split(as.data.frame(varlst, stringsAsFactors=FALSE), varlst[.grps], drop=TRUE, sep="_&&_")
                # fix problematic characters in filenames: ?*<>|+ etc
                names(datlst) <- sapply(names(datlst), URLencode, reserved=TRUE)
                filelst <- paste(.fname, paste(.grps, collapse="_"), names(datlst), "xdf", sep=".")
                for(i in seq_along(datlst))
                {
                    out <- if(file.exists(filelst[i]))
                        rxDataStep(datlst[[i]], filelst[i], append="rows")
                    else rxDataStep(datlst[[i]], filelst[i], append="none")
                }
                .outFiles <<- base::union(.outFiles, filelst)
                NULL
            },
            transformObjects=list(.grps=grps, .fname=fname, .outFiles=character(0)),
            returnTransformObjects=TRUE)$.outFiles

        sapply(sort(lst), function(obj) tbl(RxXdfData(obj), hasTblFile=TRUE), simplify=FALSE)
    }
}


delete_split_outputs <- function(fname, grps)
{
    dname <- dirname(fname)
    fname <- basename(fname)
    pattern <- paste(fname, paste(grps, collapse="_"), sep=".")
    existingFiles <- grep(pattern, dir(dname), value=TRUE, fixed=TRUE)

    if(length(existingFiles) > 0)
    {
        # should never happen, warn if it does
        warning("target files for splitting currently exist, will be overwritten")
        unlink(file.path(dname, existingFiles))
    }
}

