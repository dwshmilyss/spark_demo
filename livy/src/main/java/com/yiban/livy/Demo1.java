package com.yiban.livy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.cloudera.livy.examples.PiJob;
import org.apache.spark.api.java.function.*;

import com.cloudera.livy.*;


/**
 * test
 *
 * @auther WEI.DUAN
 * @date 2017/7/5
 * @website http://blog.csdn.net/dwshmilyss
 */
public class Demo1 {
    public static void main(String[] args){

        System.out.println(Demo1.class.getClassLoader().getResource("livy-examples-0.4.0-SNAPSHOT.jar").getPath());
        LivyClient client = null;
        try {
            String livyUrl = "http://10.21.3.78:8998";
            client = new LivyClientBuilder()
                    .setURI(new URI(livyUrl))
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        try {
            String piJar = Demo1.class.getClassLoader().getResource("livy-examples-0.4.0-SNAPSHOT.jar").getPath();
            System.err.printf("Uploading %s to the Spark context...\n", piJar);
            client.uploadJar(new File(piJar)).get();
            int samples = 10;
            System.err.printf("Running PiJob with %d samples...\n", samples);
            double pi = client.submit(new PiJob(samples)).get();

            System.out.println("Pi is roughly: " + pi);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            client.stop(true);
        }
    }
}

