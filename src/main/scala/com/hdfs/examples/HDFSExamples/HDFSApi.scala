package com.hdfs.examples.HDFSExamples
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.IOException

class HDFSApi {

  def fileSystem(): FileSystem = {
    val conf = new Configuration()
    val maprfsCoreSitePath = new Path("/Users/hadoop/hadoop-2.5.2/etc/hadoop/core-site.xml")
    val hdfsSitePath = new Path("/Users/hadoop/hadoop-2.5.2/etc/hadoop/hdfs-site.xml")
    val mapredSitePath = new Path("/Users/hadoop/hadoop-2.5.2/etc/hadoop/mapred-site.xml")

    //Add the resources to Configuration instance
    conf.addResource(maprfsCoreSitePath)
    conf.addResource(hdfsSitePath)
    conf.addResource(mapredSitePath)
    val fileSystem = FileSystem.get(conf)
    return fileSystem
  }

  /**
   * List hadoop directories
   */
  def listFiles(path: String): Unit = {
    try {
      //Get a FileSystem handle
      var pPath = new Path(path)
      var list = fileSystem.listFiles(pPath, false)
      while (list.hasNext()) {
        var f = list.next();
        println(f.getPath)
        if (!f.isDirectory() && f.getPath().getName().endsWith("jar")) {
          println()
          //DistributedCache.addFileToClassPath(f.getPath(), config);
        }
      }
    } catch {
      case ex: Exception => {
        println("Missing file exception", ex)
      }

      case ex: IOException => {
        println("IO Exception", ex)
      }
    }finally {
         println("Reading Done ------------> ")
    }
  }

  /**
   * Reading hadoop file.
   */
  def readListOfFiles(path: String): Unit = {
    println("Reading Started ------------>")

    try {
      var pPath = new Path(path)
      var status = fileSystem.listStatus(pPath);
      status.foreach { x =>
        println(x.getPath)
        var br = new BufferedReader(new InputStreamReader(fileSystem.open(x.getPath())));

        var line = br.readLine()
        while (line != null) {
          System.out.println(line);
          line = br.readLine();
        }

      }
    } catch {
      case ex: Exception => {
        println("Missing file exception", ex)
      }

      case ex: IOException => {
        println("IO Exception", ex)
      }
    }finally {
         println("Reading Done ------------> ")
    }

  }

}