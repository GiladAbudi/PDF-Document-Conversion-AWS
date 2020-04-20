//String linesPerWorker = args[2];
////        try{
////            Integer.parseInt(linesPerWorker);
////        }catch(Exception e){
////            System.out.println("bad argument for n");
////            System.exit(-1);
////        }
////        String outputName = args[1];//args [2]
////        String inputFile = args[0]; // args[1]
////        boolean terminate = false; //args[4]
////        if(args[args.length-1].equals("terminate")){
////            terminate=true;
////        }
//        if(args.length!=3 &&args.length!=4){
//        System.out.println("invalid num of arguments");
//        System.exit(-1);
//        }
package Actors;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Manager {
    private static S3Client s3;
    private static SqsClient sqs;
    private static Ec2Client ec2;
    private static final String appManagerQueue = "appManagerQueue";
    private static final String managerAppQueue = "managerAppQueue";
    private static final String workerIQ = "M2W";
    private static final String workerOQ = "W2M";
    private static final String workerJar = "https://appbucket305336117.s3.amazonaws.com/worker.jar";
    private static final String workerAmi = "ami-076515f20540e6e0b";
    private static int workerInstances;

    public static void main(String[] args) {
        System.out.println("Started manager");
        workerInstances = 0;
        System.out.println("Creating thread pool");
        ExecutorService executor = Executors.newFixedThreadPool(10);
        Region region = Region.US_EAST_1;
        System.out.println("Creating sqs client");
        sqs = SqsClient.builder().region(region).build();
        System.out.println("Creating s3 client");
        s3 = S3Client.builder().region(region).build();
        System.out.println("Creating ec2 client");
        ec2 = Ec2Client.builder().region(region).build();
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(appManagerQueue)
                .build();
        String appQueueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(managerAppQueue)
                .build();
        String toAppUrl = sqs.getQueueUrl((getQueueRequest)).queueUrl();
        System.out.println("creating first queue");
        String workerIQUrl = createQueue(workerIQ, sqs);
        System.out.println("creating second queue");
        String workerOQUrl = createQueue(workerOQ, sqs);
        CleanQueues(workerIQUrl, workerOQUrl);
        System.out.println("Manager: created and cleaned queues");
        boolean terminate = false;
        while (!terminate) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(appQueueUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            String fileLink;
            String bucket = "";
            String key = "";
            String appId = "";
            int linesPerWorker = 0;
            for (Message m : messages) {
                String body = m.body();
                System.out.println("Manager: received messeage from app: " + body);
                if (body.contains("New Task")) {
                    String[] split = body.split("#");
                    bucket = split[1];
                    key = split[2];
                    linesPerWorker = Integer.parseInt(split[3]);
                    appId = split[4];
                    System.out.println("the msg is : new Task - with appId: " + appId);
                    if (split.length > 5) {
                        System.out.println("Manager received terminate");
                        terminate = true;
                    }
                    try {
                        File input = new File("input" + appId + ".txt");
                        if (!input.exists()) {
                            input.createNewFile();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.out.println(e.getMessage());
                    }
                    fileLink = "https://" + bucket + ".s3.amazonaws.com/" + key;
                    try (BufferedInputStream in = new BufferedInputStream(new URL(fileLink).openStream());
                         FileOutputStream fileOutputStream = new FileOutputStream("input" + appId + ".txt")) {
                        byte[] dataBuffer = new byte[1024];
                        int bytesRead;
                        while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                            fileOutputStream.write(dataBuffer, 0, bytesRead);
                        }
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                    DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
                    s3.deleteObject(deleteObjectRequest);
                    BufferedReader reader;
                    int linesCounter = 0;

                    try {
                        reader = new BufferedReader(new FileReader("input" + appId + ".txt"));
                        String line = reader.readLine();
                        while (line != null) {
                            linesCounter++;
                            // read next line
                            line = reader.readLine();
                        }
                        LineHandler handler = new LineHandler(workerIQUrl,appId,"input" + appId + ".txt");
                        executor.execute(handler);
                        if (!(workerInstances > 7)) {
                            int workersToCreate = linesCounter / linesPerWorker;
                            System.out.println(workersToCreate + "Workers needed by app. current number of workers running: " +workerInstances);
                            if (!(workersToCreate < workerInstances)) {
                                if (workersToCreate + workerInstances > 7) {
                                    createWorkers(8 - workerInstances);
                                } else if (workersToCreate == 0 && workerInstances == 0) {
                                    createWorkers(1);
                                } else {
                                    createWorkers(workersToCreate - workerInstances);
                                }
                            }
                        }
//                        reader.close();
//                        File f = new File("input" + appId + ".txt");
//                        f.delete();

                    } catch (IOException e) {
                        e.printStackTrace();
                        System.out.println("Got Exception while reading input file" + e.getMessage());
                    }
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(appQueueUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteRequest);
                    AppHandler handler = new AppHandler(linesCounter, workerOQUrl, appId, bucket, key, toAppUrl);
                    executor.execute(handler);
                    waitForWorkersToStart();
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        System.out.println("got interrupted exception " + e.getMessage());
                    }

                    if (terminate) {
                        System.out.println("manager received terminate from local app, terminating");
                        executor.shutdown();
                        try {
                            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                        } catch (InterruptedException e) {
                            System.out.println("got interrupted exception " + e.getMessage());
                        }
                        closeInstances("Worker");
                        terminate = true;
                        break;
                    }
                } else {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        System.out.println("got interrupted exception " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
            //TODO - check if all instances are alive
        }
        closeInstances("Manager");
    }

    private static void waitForWorkersToStart() {
        System.out.println("waiting for workers to start");
        String nextToken = null;
        boolean found = true;
        while (found) {
            found = false;
            try {
                do {
                    DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                    DescribeInstancesResponse response = ec2.describeInstances(request);
                    for (Reservation reservation : response.reservations()) {
                        for (Instance instance : reservation.instances()) {
                            List<Tag> tags = instance.tags();
                            if (tags != null) {
                                for (Tag tag : tags) {
                                    if (tag.value().equals("Worker") && instance.state().name().toString().equals("pending")) {
                                        found = true;
                                    }

                                }
                            }
                        }
                    }
                    nextToken = response.nextToken();
                }
                while (nextToken != null);

            } catch (Ec2Exception e) {
                System.out.println("got exception while waiting for workers to start " + e.getMessage());
                e.getStackTrace();
            }
        }
    }

    private static void closeInstances(String role) {
        System.out.println("closing instances of " +role);
        // snippet-start:[ec2.java2.describe_instances.main]
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        List<Tag> tags = instance.tags();
                        if (tags != null) {
                            for (Tag tag : tags) {
                                if (tag.value().equals(role) && (instance.state().name().toString().equals("running") || instance.state().name().toString().equals("pending"))) {
                                    TerminateInstancesRequest req = TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build();
                                    TerminateInstancesResponse res = ec2.terminateInstances(req);
                                }
                            }
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (
                Ec2Exception e) {
            System.out.println("got exception while closing worker instances " + e.getMessage());
            e.getStackTrace();
        }
    }

    public static void handleInputLine(String queue, String line, String appId) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queue)
                .messageBody(appId + "#" + line)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    public static void handleWorkersOutput(int counter, String queue, String appId, String bucket, String key, String appQueue) {
        int lineCount = counter;
        String outputFile = "output" + appId + ".html";
        while (lineCount != 0) {
            try {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queue)
                        .build();
                List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
                for (Message m : messages) {
                    String body = m.body();
                    if (body.contains("PDF task done")) {
                        System.out.println("Manager: received pdf task done message: " + body);
                        String[] split = body.split("#", 3);
                        String line = split[2];
                        String currId = split[1];
                        if (currId.equals(appId)) {
                            writeLineToOutput(line, outputFile);
                            lineCount--;
                            System.out.println("after counter -- , linecounter = : " + lineCount);
                            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                                    .queueUrl(queue)
                                    .receiptHandle(m.receiptHandle())
                                    .build();
                            sqs.deleteMessage(deleteRequest);
                            System.out.println("delete request from queueURL : " + queue);
                        }
                    }
                }
            }
            catch(Exception e){
                System.out.println("receieved Exception while reading PDF done from workers: " +e.getMessage());
            }
        }
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(outputFile).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(outputFile)));
        System.out.println("upload to S3  : " + outputFile);
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(appQueue)
                .messageBody("Done task#" + appId)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
        File f = new File(outputFile);
        f.delete();
        System.out.println("delete outputFile : " + outputFile);
    }


    private static void writeLineToOutput(String line, String outputFile) {
        try {
            FileWriter fw = new FileWriter(outputFile, true); //the true will append the new data
            fw.write("<p>" + line + "</p>");
            fw.close();
            System.out.println("write line to out put- line : " + line + "\n");
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
        }
    }

    private static void CleanQueues(String queue1, String queue2) {
        sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(queue1).build());
        sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(queue2).build());
        System.out.println("clean queues\n");
    }

    public static String createQueue(String queue, SqsClient sqs) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue)
                    .build();
            sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            System.out.println("received queue exists exception");
            throw e;
        }
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private static String getWorkerData() {
        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash");
        lines.add("cd home/ec2-user/");
        lines.add("wget " + workerJar + " -O worker.jar");
        lines.add("java -Xmx30g -jar worker.jar &> log.txt");
        return new String(Base64.getEncoder().encode(join(lines, "\n").getBytes()));

    }

    public static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

    public static void createWorkers(int numberOfWorkersToCreate) {
        System.out.println("Manager: creating " + numberOfWorkersToCreate + " worker ec2 instances");
        if (numberOfWorkersToCreate <= 0) {
            return;
        }
        try {
            for (int i = 0; i < numberOfWorkersToCreate; i++) {
                workerInstances++;
                RunInstancesRequest runRequest = RunInstancesRequest.builder()
                        .imageId(workerAmi)
                        .instanceType(InstanceType.T2_SMALL)
                        .maxCount(1)
                        .minCount(1)
                        .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::577569430471:instance-profile/workerRole").build())
                        .securityGroups("eilon")
                        .keyName("eilon")
                        .userData(getWorkerData())
                        .build();
                RunInstancesResponse response = ec2.runInstances(runRequest);
                String instanceId = response.instances().get(0).instanceId();
                Tag tag = Tag.builder()
                        .key("role")
                        .value("Worker")
                        .build();

                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                        .resources(instanceId)
                        .tags(tag)
                        .build();
                ec2.createTags(tagRequest);

            }
        } catch (Ec2Exception e) {
            System.err.println("Ec2 exception : " + e.getMessage());
            System.exit(1);
        }
    }
}