/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jclouds.examples.google.cloudstorage;

import com.google.common.io.Closeables;
import com.google.common.io.Files;

import org.jclouds.ContextBuilder;
import org.jclouds.googlecloudstorage.GoogleCloudStorageApi;
import org.jclouds.googlecloudstorage.domain.Bucket;
import org.jclouds.googlecloudstorage.domain.BucketTemplate;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.jclouds.examples.google.cloudstorage.Constants.PROVIDER;

/**
 * This example manages buckets of Google Cloud Storage.
 */
public class ManageBuckets implements Closeable {
   private final GoogleCloudStorageApi cloudStorageApi;

   /**
    * To get a service account and its private key see [TODO: write some
    * documentation on the website and put a link to it]
    *
    * The first argument (args[0]) is your service account email address
    *    (https://developers.google.com/console/help/new/#serviceaccounts).
    * The second argument (args[1]) is a path to your service account private key PEM file without a password. It is
    *    used for server-to-server interactions (https://developers.google.com/console/help/new/#serviceaccounts).
    *    The key is not transmitted anywhere.
    * The third argument (args[2]) is the command you want to perform: "create", "delete", or "list".
    * The fourth argument (args[3]) is the project name
    *    (see https://developers.google.com/storage/docs/signup#activate).
    *    This argument is skipped for command "delete".
    * The fifth argument (args[4]) is the name of the bucket that you want to create or delete.
    *    This argument is skipped for command list.
    *    NOTE: Bucket names must be unique across the entire Google Cloud Storage namespace
    *    (https://developers.google.com/storage/docs/bucketnaming#requirements).
    *
    * Examples:
    *
    * java org.jclouds.examples.google.cloudstorage.ManageBuckets \
    *    somecrypticname@developer.gserviceaccount.com \
    *    /home/planetnik/Work/Cloud/OSS/certificate/gcp-oss.pem \
    *    create \
    *    myprojectname \
    *    planetnikbucketname
    *
    * java org.jclouds.examples.google.cloudstorage.ManageBuckets \
    *    somecrypticname@developer.gserviceaccount.com \
    *    /home/planetnik/Work/Cloud/OSS/certificate/gcp-oss.pem \
    *    delete \
    *    planetnikbucketname
    *
    * java org.jclouds.examples.google.cloudstorage.ManageBuckets \
    *    somecrypticname@developer.gserviceaccount.com \
    *    /home/planetnik/Work/Cloud/OSS/certificate/gcp-oss.pem \
    *    list \
    *    myprojectname
    */
   public static void main(final String[] args) {
      String serviceAccountEmailAddress = args[0];
      String serviceAccountKey = null;
      try {
         serviceAccountKey = Files.toString(new File(args[1]), Charset.defaultCharset());
      } catch (IOException e) {
         System.err.println("Cannot open service account private key PEM file: " + args[1] + "\n" + e.getMessage());
         System.exit(1);
      }
      String command = args[2];
      String projectName = null;
      String bucketName = null;
      if (command.equals("create")) {
         projectName = args[3];
         bucketName = args[4];
      } else if (command.equals("delete")) {
         bucketName = args[3];
         if (args.length >= 5) {
            System.err.println("Command 'delete' require only one additional parameter (bucketName).");
            System.exit(1);
         }
      } else if (command.equals("list")) {
         projectName = args[3];
         if (args.length >= 5) {
            System.err.println("Command 'list' require only one additional parameters (projectName).");
            System.exit(1);
         }
      } else {
         System.err.println("Unknown command: " + command);
         System.exit(1);
      }
      ManageBuckets manageBuckets = new ManageBuckets(serviceAccountEmailAddress, serviceAccountKey);

      if (command.equals("create")) {
         manageBuckets.createBucket(projectName, bucketName);
      } else if (command.equals("delete")) {
         manageBuckets.deleteBucket(bucketName);
      } else if (command.equals("list")) {
         manageBuckets.listBuckets(projectName);
      } else {
         try {
            manageBuckets.close();
         } catch (IOException e) {
            e.printStackTrace();
         }
         throw new RuntimeException("Should never happen.");
      }

      try {
         manageBuckets.close();
      } catch (IOException e) {
         e.printStackTrace();
         System.exit(1);
      }
   }

   public ManageBuckets(final String serviceAccountEmailAddress, final String serviceAccountKey) {
      cloudStorageApi = ContextBuilder.newBuilder(PROVIDER)
            .credentials(serviceAccountEmailAddress, serviceAccountKey)
            .buildApi(GoogleCloudStorageApi.class);
      /*
      // This works fine:
      // System.out.println("result: " + cloudStorageApi.getBucketApi().listBucket("googpl-gcp-oss-7").size());
      ///
      cloudStorageApi.getBucketApi().createBucket("googpl-gcp-oss-7", new BucketTemplate().name("marekws"));
      System.out.println("result: " + cloudStorageApi.getBucketApi().listBucket("googpl-gcp-oss-7").size());
      //
       */
   }

   /**
    * Creates a bucket in a given project.
    * @param projectName Name of the project in which the bucket should be created.
    * @param bucketName Name of the bucket to create.
    */
   public final void createBucket(final String projectName, final String bucketName) {
      Bucket bucket = null;
      try {
         bucket = cloudStorageApi.getBucketApi().createBucket(projectName, new BucketTemplate().name(bucketName));
      } catch (RuntimeException e) {
         System.err.println("Creating bucket " + bucketName + " failed.\n" + e.getMessage());
         System.exit(1);
      }
      if (bucket != null) {
         System.out.print("Bucket " + bucket.getName() + " successfully created in project " + projectName + " .");
      } else {
         System.err.println("Creating bucket " + bucketName + "failed.");
         System.exit(1);
      }
   }

   /**
    * Deletes a bucket.
    * @param bucketName Name of the bucket to delete.
    */
   public final void deleteBucket(final String bucketName) {
      try {
         cloudStorageApi.getBucketApi().deleteBucket(bucketName);
         System.out.print("Bucket " + bucketName + " successfully deleted.");
      } catch (RuntimeException e) {
         System.err.println("Deleting bucket " + bucketName + " failed.\n" + e.getMessage());
         System.exit(1);
      }
   }

   /**
    * Lists all buckets in a given project.
    * @param projectName Name of the project in which the buckets should be listed.
    */
   public final void listBuckets(final String projectName) {
      System.out.println("List of buckets for project " + projectName + ":");
      for (Bucket bucket : cloudStorageApi.getBucketApi().listBucket(projectName)) {
         System.out.println("* " + bucket.getName());
      }
   }

   /**
    * Always close your service when you're done with it.
    *
    * Note that closing quietly like this is not necessary in Java 7.
    * You would use try-with-resources in the main method instead.
    */
   public final void close() throws IOException {
      Closeables.close(cloudStorageApi, true);
   }
}
