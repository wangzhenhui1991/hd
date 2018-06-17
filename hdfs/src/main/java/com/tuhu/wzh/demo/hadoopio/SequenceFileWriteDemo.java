/*
 * Copyright 2018 tuhu.cn All right reserved. This software is the
 * confidential and proprietary information of tuhu.cn ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Tuhu.cn
 */

package com.tuhu.wzh.demo.hadoopio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

/**
 * @author wangzhenhui
 * @date 2018/6/12 001221:00
 */
public class SequenceFileWriteDemo {


    private static final String[] DATA={
            "one,two, open Intellij",
            "three,four, coding my world"
    };


    public static  void main(String[] args) throws IOException {
        String uri = "";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);

        Path path = new Path(uri);

        IntWritable key =new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer =null;

        writer = SequenceFile.createWriter(fs,conf,path,key.getClass(),value.getClass());



    }

}
