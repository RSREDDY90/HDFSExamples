package com.hdfs.examples.HDFSExamples

object ListHdfsFilesTest {
  def main(args: Array[String]): Unit = {
    println("test")  
    
    val hdfsApi = new HDFSApi()
    //hdfsApi.listFiles("/user")
    hdfsApi.readListOfFiles("/user")
  }
  
}