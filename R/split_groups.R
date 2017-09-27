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
    cleanSplitOutput(fname, grps, fs)

    # mimic behaviour of rxSplit: rxDataStep that splits each chunk, calls rxDataStep on each split
    filelst <- rxDataStep(data, transformFunc=function(varlst) {
            if(.rxIsTestChunk)
                return(NULL)
            datlst <- split(as.data.frame(varlst, stringsAsFactors=FALSE), varlst[.grps], drop=TRUE, sep="--")
            # fix problematic characters in filenames: ?*<>|+ etc
            names(datlst) <- sapply(names(datlst), URLencode, reserved=TRUE)

            filelst <- paste(.fname, paste(.grps, collapse="-"), names(datlst), sep="--")
            if(!.composite)
                filelst <- paste0(filelst, ".xdf")
            outlst <- lapply(filelst, RxXdfData, fileSystem=.fs, createCompositeSet=.composite)

            for(i in seq_along(datlst))
            {
                append <- if(file.exists(filelst[i])) "rows" else "none"
                out <- rxDataStep(datlst[[i]], outlst[[i]], append=append)
            }
            .outFiles <<- base::union(.outFiles, filelst)
            NULL
        },
        transformObjects=list(.grps=grps, .fname=fname, .outFiles=character(0), .fs=fs, .composite=composite),
        returnTransformObjects=TRUE)$.outFiles

    sapply(sort(filelst), function(f) tbl_xdf(file=f, fileSystem=fs, createCompositeSet=composite))
}


cleanSplitOutput <- function(fname, grps, fileSystem)
{
    dname <- dirname(fname)
    fname <- basename(fname)
    pattern <- paste(fname, paste(grps, collapse="-"), sep="--")

    hd <- in_hdfs(fileSystem)
    existingFiles <- if(hd)
        grep(pattern, hdfs_dir(dname), value=TRUE, fixed=TRUE)
    else grep(pattern, dir(dname), value=TRUE, fixed=TRUE)

    if(length(existingFiles) == 0)
        return()

    message("removing old temporary files from splitting")
    if(hd)
        hdfs_dir_remove(file.path(dname, existingFiles))
    else unlink(file.path(dname, existingFiles))
}


