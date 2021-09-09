package com.yiban.spark.hive.scala.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, Trash}

import java.io.{File, IOException}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Operations of fs
 * Author: karl
 * Date:20.09.24
 */
object FSUtils {
    private val pathList = new util.ArrayList[String]()
    var fs: FileSystem = _

    /**
     * Load HDFS Hudi formatted files
     */
    def listHDFSFiles(hdfsPath: String): List[String] = {
        setFs()
        val path = new Path(hdfsPath)
        val fileStatuses = fs.listStatus(path)
        for (fileStatus <- fileStatuses) {
            pathList.add(fileStatus.getPath.toString)
        }
        pathList.asScala.toList
    }

    def listHDFSFilesByScala(hdfsPath: String): List[String] = {
        val pathList = new ListBuffer[String]()
        setFs()
        val path = new Path(hdfsPath)
        val fileStatuses = fs.listStatus(path)
        for (fileStatus <- fileStatuses) {
            pathList.append(fileStatus.getPath.toString)
        }
        pathList.toList
    }

    /**
     * Judge if Hfile exists.
     */
    def hPathExist(hdfsPath: String): Boolean = {
        setFs()
        val path = new Path(hdfsPath)
        fs.exists(path)
    }

    def createDir(hdfsPath: String) : Boolean = {
        setFs()
        val path = new Path(hdfsPath)
        fs.mkdirs(path)
    }

    /**
     * Load local Hudi formatted files
     */
    def listFiles(ph: String): List[String] = {
        val file = new File(ph)
        val files = file.listFiles()
        for (file <- files) {
            pathList.add(file.getAbsolutePath)
        }
        pathList.asScala.toList
    }

    /**
     * Judge if file exists.
     */
    def pathExist(path: String): Boolean = {
        val file = new File(path)
        file.exists()
    }

    /**
     * delete hdfs file
     */
    def deleteFile(hdfsPath: String, recursive: Boolean): Boolean = {
        setFs()
        val path = new Path(hdfsPath)
        fs.delete(path, recursive)
    }

    def mvFile(hdfsPath: String,hdfsPathTag: String):Boolean ={
        setFs()
        val path = new Path(hdfsPath)
        val pathTag = new Path(hdfsPathTag)
        fs.rename(path,pathTag)
    }

    def setFs(): Unit = {
        val conf = new Configuration()
        conf.set("fs.defaultFS", "hdfs://hdp1-test.leadswarp.com:8020")
        fs = FileSystem.get(conf)
    }

    def setFs(user:String): Unit = {
        val conf = new Configuration()
        conf.set("fs.defaultFS","hdfs://hdp1-test.leadswarp.com:8020")
        System.setProperty("HADOOP_USER_NAME",user)
        fs = FileSystem.get(conf)
    }

    def copyFile(hdfsPath: String,hdfsPathTag: String):Boolean ={
        setFs()
        val path = new Path(hdfsPath)
        val pathTag = new Path(hdfsPathTag)
        if (fs.exists(pathTag)){
            fs.delete(pathTag,true)
        }
        FileUtil.copy(fs,path,fs,pathTag,false,true,fs.getConf)
    }

    /**
     * Delete a file/directory on hdfs,and move a file/directory to Trash
     * @param fs
     * @param path
     * @param recursive
     * @param skipTrash
     * @return
     * @throws IOException
     */
    def deleteFile(hdfsPath: String, recursive: Boolean,skipTrash:Boolean) : Boolean = {
        setFs()
        val path = new Path(hdfsPath)
        if (!skipTrash) {
            val trashTmp = new Trash(fs, fs.getConf)
            if (trashTmp.moveToTrash(path)) {
                println("Moved to trash: " + path)
                return true
            }
        }
        fs.delete(path, recursive)
    }


}
