splitGroups <- function(data, outXdf=data)
{
    grps <- group_vars(data)
    on.exit(deleteIfTbl(data))
    # no splitting on Hdfs
    if(inherits(rxGetFileSystem(data), "RxHdfsFileSystem"))
        stop("splitting groups not supported on HDFS")

    fname <- tools::file_path_sans_ext(rxXdfFileName(outXdf))
    fname <- file.path(get_dplyrxdf_dir(), basename(fname))
    composite <- is_composite_xdf(data)

    # if files exist that could interfere with splitting output, delete them
    # should never be necessary because base filename is a randomly generated tempfile
    # and each split should be followed by a combine
    deleteSplitOutputs(fname, grps)

    # mimic behaviour of rxSplit: rxDataStep that splits each chunk, calls rxDataStep on each split
    filelst <- rxDataStep(data, transformFunc=function(varlst) {
            datlst <- split(as.data.frame(varlst, stringsAsFactors=FALSE), varlst[.grps], drop=TRUE, sep="_&&_")
            # fix problematic characters in filenames: ?*<>|+ etc
            names(datlst) <- sapply(names(datlst), URLencode, reserved=TRUE)

            filelst <- paste(.fname, paste(.grps, collapse="#"), names(datlst), sep="##")
            outlst <- lapply(filelst, RxXdfData, createCompositeSet=.composite)

            for(i in seq_along(datlst))
            {
                out <- if(file.exists(filelst[i]))
                    rxDataStep(datlst[[i]], outlst[[i]], append="rows")
                else rxDataStep(datlst[[i]], outlst[[i]], append="none")
            }
            .outFiles <<- base::union(.outFiles, filelst)
            NULL
        },
        transformObjects=list(.grps=grps, .fname=fname, .outFiles=character(0), .composite=composite),
        returnTransformObjects=TRUE)$.outFiles

    sapply(sort(filelst), function(f) tbl_xdf(file=f, createCompositeSet=composite))
}


deleteSplitOutputs <- function(fname, grps)
{
    dname <- dirname(fname)
    fname <- basename(fname)
    pattern <- paste(fname, paste(grps, collapse="_"), sep="__")
    existingFiles <- grep(pattern, dir(dname), value=TRUE, fixed=TRUE)

    if(length(existingFiles) > 0)
    {
        # should never happen, warn if it does
        warning("target files for splitting currently exist, will be overwritten")
        unlink(file.path(dname, existingFiles))
    }
}
