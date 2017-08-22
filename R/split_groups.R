splitGroups <- function(data)
{
    grps <- group_vars(data)
    on.exit(deleteIfTbl(data))

    fs <- rxGetFileSystem(data)
    fname <- tools::file_path_sans_ext(data@file)
    fname <- file.path(get_dplyrxdf_dir(fs), basename(fname))
    composite <- is_composite_xdf(data)

    # if files exist that could interfere with splitting output, delete them
    # this can happen, eg if a previous splitting op failed
    cleanSplitOutput(fname, grps)

    # mimic behaviour of rxSplit: rxDataStep that splits each chunk, calls rxDataStep on each split
    filelst <- rxDataStep(data, transformFunc=function(varlst) {
            datlst <- split(as.data.frame(varlst, stringsAsFactors=FALSE), varlst[.grps], drop=TRUE, sep="--")
            # fix problematic characters in filenames: ?*<>|+ etc
            names(datlst) <- sapply(names(datlst), URLencode, reserved=TRUE)

            filelst <- paste(.fname, paste(.grps, collapse="#"), names(datlst), sep="##")
            outlst <- lapply(filelst, RxXdfData, fileSystem=.fs, createCompositeSet=.composite)

            for(i in seq_along(datlst))
            {
                out <- if(file.exists(filelst[i]))
                    rxDataStep(datlst[[i]], outlst[[i]], append="rows")
                else rxDataStep(datlst[[i]], outlst[[i]], append="none")
            }
            .outFiles <<- base::union(.outFiles, filelst)
            NULL
        },
        transformObjects=list(.grps=grps, .fname=fname, .outFiles=character(0), .fs=fs, .composite=composite),
        returnTransformObjects=TRUE)$.outFiles

    sapply(sort(filelst), function(f) tbl_xdf(file=f, fileSystem=fs, createCompositeSet=composite))
}


cleanSplitOutput <- function(fname, grps)
{
    dname <- dirname(fname)
    fname <- basename(fname)
    pattern <- paste(fname, paste(grps, collapse="#"), sep="##")
    existingFiles <- grep(pattern, dir(dname), value=TRUE, fixed=TRUE)

    if(length(existingFiles) > 0)
    {
        message("removing old temporary files from splitting")
        unlink(file.path(dname, existingFiles))
    }
}
