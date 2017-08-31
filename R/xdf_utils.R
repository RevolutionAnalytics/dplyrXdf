#' Utility functions for working with Xdf files
#'
#' Copy, move, rename and delete an Xdf file.
#'
#' @param src For \code{copy_xdf}, \code{move_xdf} and \code{rename_xdf}, an Xdf data source object (\emph{not} a filename).
#' @param dest For \code{copy_xdf}, \code{move_xdf} and \code{rename_xdf}, a character string giving the destination file. Note that for \code{rename_xdf}, this should only be a base name, not a full path.
#'
#' @details
#' The \code{copy_xdf} function copies the Xdf file given by \code{src} to the location specified by \code{dest}, possibly renaming it as well; \code{move_xdf} moves the file. \code{rename_xdf} does a strict rename (the location of the file is unchanged, only its name). \code{delete_xdf} deletes the file given by \code{xdf}.
#'
#' These are utility functions for working with the files/directories where the data for an Xdf data source is stored. They make use of low-level OS functionality, so should be more efficient that running \code{rxDataStep}. They work with both standard and composite Xdf files, and with data stored both in the native filesystem and in HDFS.
#'
#' @return
#' \code{copy_xdf}, \code{move_xdf} and \code{rename_xdf} return an Xdf data source object pointing to the new file location. \code{delete_xdf} returns TRUE if the delete operation succeeded, FALSE otherwise.
#'
#' @seealso
#' \code{\link{file.copy}}, \code{\link{file.rename}}, \code{\link{unlink}},
#' \code{\link{rxHadoopCopy}}, \code{\link{rxHadoopMove}}, \code{\link{rxHadoopRemove}}, \code{\link{rxHadoopRemoveDir}}
#' @rdname xdf_utils
#' @export
copy_xdf <- function(src, dest)
{
    copyOrMove(src, dest, move=FALSE)
}


#' @rdname xdf_utils
#' @export
move_xdf <- function(src, dest)
{
    copyOrMove(src, dest, move=TRUE)
}


#' @rdname xdf_utils
#' @export
rename_xdf <- function(src, dest)
{
    if(dirname(dest) == dirname(src@file))
        dest <- basename(dest)
    else if(basename(dest) != dest)
        stop("to move an Xdf file to a new location, use move_xdf", call.=FALSE)

    composite <- is_composite_xdf(src)
    if(in_hdfs(src))
    {
        if(!composite)
            stop("only composite Xdf files supported in HDFS")

        destPath <- normalizeHdfsPath(file.path(dirname(src@file), dest))
        host <- hdfs_host(src)
        hdfs_file_move(src@file, destPath, host=host)

        # rename all files in data and metadata subdirs
        pat <- sprintf("^%s", basename(src@file))
        dataFiles <- hdfs_dir(destPath, full_path=TRUE, recursive=TRUE, host=host)
        dataDirs <- dirname(dataFiles)
        newDataFiles <- file.path(dataDirs, sub(pat, dest, basename(dataFiles)))
        hdfs_file_move(dataFiles, newDataFiles, host=host)
    }
    else
    {
        destPath <- file.path(dirname(src@file), dest)
        file.rename(src@file, destPath)
        if(composite)
        {
            # rename all files in data and metadata subdirs
            pat <- sprintf("^%s", basename(src@file))
            dataFiles <- dir(destPath, full.names=TRUE, recursive=TRUE)
            dataDirs <- dirname(dataFiles)
            newDataFiles <- file.path(dataDirs, sub(pat, dest, basename(dataFiles)))
            file.rename(dataFiles, newDataFiles)
        }
    }
    modifyXdf(src, file=destPath, createCompositeSet=composite)
}


#' @param xdf For \code{delete_xdf}, an Xdf data source object.
#' @rdname xdf_utils
#' @export
delete_xdf <- function(xdf)
{
    if(!is_xdf(xdf))
        stop("only for deleting Xdf files: input is of class ", class(xdf)[1])
    if(in_hdfs(xdf))
    {
        out <- hdfs_dir_remove(xdf@file, host=xdf@fileSystem, intern=TRUE)
        attr(out, "status")
    }
    else unlink(xdf@file, recursive=TRUE)
}


# ensure that composite xdf has no extension, normal xdf has extension
validateXdfFile <- function(filename, composite)
{
    ext <- tools::file_ext(filename)
    if(composite)
    {
        if(ext != "")
            filename <- tools::file_path_sans_ext(filename)
    }
    else
    {
        if(ext != "xdf")
            filename <- paste0(tools::file_path_sans_ext(filename), ".xdf")
    }
    filename
}


# create a new Xdf data source based on an existing Xdf
modifyXdf <- function(xdf, file=xdf@file, varsToKeep=xdf@colNames, varsToDrop=NULL,
    blocksPerRead=xdf@rowsOrBlocksPerRead, fileSystem=xdf@fileSystem,
    createCompositeSet=xdf@createCompositeSet, blocksPerCompositeFile=xdf@blocksPerCompositeFile, ...)
{
    if(is.null(createCompositeSet))
        createCompositeSet <- is_composite_xdf(xdf)
    file <- validateXdfFile(file, createCompositeSet)
    if(in_hdfs(xdf))
        file <- gsub("\\\\", "/", file) # rm backslash cruft

    if(identical(varsToKeep, ""))
        varsToKeep <- NULL

    RxXdfData(file=file, varsToKeep=varsToKeep, varsToDrop=varsToDrop,
              blocksPerRead=blocksPerRead, fileSystem=fileSystem,
              createCompositeSet=createCompositeSet, blocksPerCompositeFile=blocksPerCompositeFile, ...)
}
