% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/hdfs_filetransfer.R
\name{hdfs_upload}
\alias{hdfs_upload}
\alias{hdfs_download}
\title{Transfer files and directories to and from HDFS}
\usage{
hdfs_upload(src, dest, overwrite = FALSE, nativeTarget = "/tmp",
  host = hdfs_host(), ...)

hdfs_download(src, dest, overwrite = FALSE, nativeTarget = "/tmp",
  host = hdfs_host(), ...)
}
\arguments{
\item{src, dest}{Character strings giving the source and destination paths.}

\item{overwrite}{Whether to overwrite existing files at the destination.}

\item{nativeTarget}{Only when transferring to/from a remote client. The directory on the edge node in which to stage files.}

\item{...}{Other arguments to the Hadoop \code{copyFromLocal}/\code{copyToLocal} command.}
}
\value{
A logical value indicating whether the file transfer succeeded.
}
\description{
Transfer files and directories to and from HDFS
}
\details{
These functions transfer files and directories between the native filesystem and HDFS. \code{hdfs_upload} copies files from the native filesystem into HDFS, and \code{hdfs_download} does the reverse. They can be used both from the edge node of a Hadoop/Spark cluster, and from a remote client. In the latter case, the transfer is a two-stage process: for downloading, the files are copied to the edge node in the directory given by \code{nativeTarget}, and then copied to the client; and vice-versa for uploading.

Note that renaming directories as part of the transfer is supported for downloading from HDFS, but not for uploading.
}
\examples{
\dontrun{
as_xdf(mtcars, "mtcars.xdf", overwrite=TRUE)
hdfs_upload("mtcars.xdf", ".")

write.csv(mtcars, "mtcars.csv", row.names=FALSE)
hdfs_upload("mtcars.csv", "mtcars_uploaded.csv")

file.remove("mtcars.csv")
hdfs_download("mtcars_uploaded.csv", "mtcars.csv")
read.csv("mtcars.csv")

# hdfs_upload() and hdfs_download() can transfer any file, not just datasets
desc <- system.file("DESCRIPTION", package="dplyrXdf")
hdfs_upload(desc, "dplyrXdf_description")

# uploading to attached ADLS storage
hdfs_upload("mtcars.xdf", ".", host="adls.host.name")
}
}
\seealso{
\code{\link{download.file}}, \code{\link{rxHadoopCopyFromLocal}}, \code{\link{rxHadoopCopyFromClient}}
}
