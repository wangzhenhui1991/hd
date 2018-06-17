/*
 * Copyright 2018 tuhu.cn All right reserved. This software is the
 * confidential and proprietary information of tuhu.cn ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Tuhu.cn
 */

package com.tuhu.wzh.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author wangzhenhui
 * @date 2018/6/4 000416:34
 */
public class HdfsOperationIntel {


    //initialization
    static Configuration conf = new Configuration();
    static FileSystem hdfs;
    static {
        String path = "/usr/java/hadoop-1.0.3/conf/";
        conf.addResource(new Path(path + "core-site.xml"));
        conf.addResource(new Path(path + "hdfs-site.xml"));
        conf.addResource(new Path(path + "mapred-site.xml"));
        path = "/usr/java/hbase-0.90.3/conf/";
        conf.addResource(new Path(path + "hbase-site.xml"));
        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getFileSystemByConfigure(){

    }


//    public static FileSystem getFileSystemByUri(){
//        String uri = "hdfs://cdh-1:9000/user/root";
//        try {
//            return FileSystem.get(new URI(uri),conf,"root");
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (URISyntaxException e) {
//            e.printStackTrace();
//        }
//    }


    //create a direction
    public void createDir(String dir) throws IOException {
        Path path = new Path(dir);
        hdfs.mkdirs(path);
        System.out.println("new dir \t" + conf.get("fs.default.name") + dir);
    }

    //copy from local file to HDFS file
    public void copyFile(String localSrc, String hdfsDst) throws IOException{
        Path src = new Path(localSrc);
        Path dst = new Path(hdfsDst);
        hdfs.copyFromLocalFile(src, dst);

        //list all the files in the current direction
        FileStatus files[] = hdfs.listStatus(dst);
        System.out.println("Upload to \t" + conf.get("fs.default.name") + hdfsDst);
        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }

    //create a new file
    public void createFile(String fileName, String fileContent) throws IOException {
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        FSDataOutputStream output = hdfs.create(dst);
        output.write(bytes);
        System.out.println("new file \t" + conf.get("fs.default.name") + fileName);
    }

    //list all files
    public void listFiles(String dirName) throws IOException {
        Path f = new Path(dirName);
        FileStatus[] status = hdfs.listStatus(f);
        System.out.println(dirName + " has all files:");
        for (int i = 0; i< status.length; i++) {
            System.out.println(status[i].getPath().toString());
        }
    }

    //judge a file existed? and delete it!
    public void deleteFile(String fileName) throws IOException {
        Path f = new Path(fileName);
        boolean isExists = hdfs.exists(f);
        if (isExists) {	//if exists, delete
            boolean isDel = hdfs.delete(f,true);
            System.out.println(fileName + "  delete? \t" + isDel);
        } else {
            System.out.println(fileName + "  exist? \t" + isExists);
        }
    }

    public static void main(String[] args) throws IOException {
//        HdfsOperationIntel ofs = new HdfsOperationIntel();
//        System.out.println("\n=======create dir=======");
//        String dir = "/test";
//        ofs.createDir(dir);
//        System.out.println("\n=======copy file=======");
//        String src = "/home/ictclas/Configure.xml";
//        ofs.copyFile(src, dir);
//        System.out.println("\n=======create a file=======");
//        String fileContent = "Hello, world! Just a test.";
//        ofs.createFile(dir+"/word.txt", fileContent);

        Configuration conf = new Configuration();
        FileSystem hdfs;
        String uri = "hdfs://cdh-1:8022";
        try {
            hdfs = FileSystem.get(new URI(uri),conf,"root");

            Path f = new Path("/user/root");
            FileStatus[] status = hdfs.listStatus(f);
            for (int i = 0; i< status.length; i++) {
                System.out.println(status[i].getPath().toString());
            }


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }
}
