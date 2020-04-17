package Actors;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager {
    private static S3Client s3;
    private static SqsClient sqs;
    private static Ec2Client ec2;
    private static final String appManagerQueue = "appManagerQueue";
    private static final String managerAppQueue = "managerAppQueue";
    private static final String workerIQ = "M2W";
    private static final String workerOQ = "W2M";

    public static void main(String args[]) {
        ec2 = Ec2Client.create();
        int workerInstances = 0;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        Region region = Region.US_EAST_1;
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(appManagerQueue)
                .build();
        String appQueueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(managerAppQueue)
                .build();
        String toAppUrl = sqs.getQueueUrl((getQueueRequest)).queueUrl();
        String workerIQUrl = createQueue(workerIQ);
        String workerOQUrl = createQueue(workerOQ);
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
                            handleInputLine(workerIQUrl, line, appId);
                            System.out.println("Manager: sent line " +line +"to workers");
                            // read next line
                            line = reader.readLine();
                        }
                        if (!(workerInstances > 19)) {
                            int workersToCreate = linesCounter / linesPerWorker;
                            if (!(workersToCreate < workerInstances)) {
                                if (workersToCreate + workerInstances > 19) {
                                    createWorkers(19 - workerInstances);
                                } else if (workersToCreate == 0) {
                                    createWorkers(1);
                                } else {
                                    createWorkers(workersToCreate);
                                }
                            }
                        }

                        reader.close();
                        File f = new File("input" + appId + ".txt");
                        f.delete();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(appQueueUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteRequest);
                    AppHandler handler = new AppHandler(linesCounter, workerOQUrl, appId, bucket, key, toAppUrl);
                    executor.execute(handler);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        System.out.println("got interrupted exception " + e.getMessage());
                    }

                    if (terminate) {
                        executor.shutdown();
                        try {
                            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                        } catch (InterruptedException e) {
                            System.out.println("got interrupted exception " + e.getMessage());
                        }
                        terminate = true;
                        break;
                    }
                } else {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            //TODO - check if all instances are alive
        }
    }

    private static void handleInputLine(String queue, String line, String appId) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queue)
                .messageBody(appId + "#" + line)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    public static void handleWorkersOutput(int counter, String queue, String appId, String bucket, String key, String appQueue) {
        int lineCount = counter;
        String outputFile = "output" + appId + ".txt";
        while (lineCount != 0) {
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
                        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                                .queueUrl(queue)
                                .receiptHandle(m.receiptHandle())
                                .build();
                        sqs.deleteMessage(deleteRequest);
                    }
                }
            }
        }
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(outputFile)));
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(appQueue)
                .messageBody("Done task#" + appId)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
        File f = new File(outputFile);
        f.delete();
    }


    private static void writeLineToOutput(String line, String outputFile) {
        try {
            FileWriter fw = new FileWriter(outputFile, true); //the true will append the new data
            fw.write(line);//appends the string to the file
            fw.close();
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
        }
    }

    private static void CleanQueues(String queue1, String queue2) {
        sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(queue1).build());
        sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(queue2).build());
    }

    private static String createQueue(String queue) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue)
                    .build();
            sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
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
//        lines.add("sudo apt-get install openjdk-8-jre-headless -y");
        lines.add("wget " + workerJar + " -O manager.jar");
        lines.add("java -jar worker.jar &> log.txt");
        return new String(Base64.getEncoder().encode(join(lines, "\n").getBytes()));

    }

    private static String join(Collection<String> s, String delimiter) {
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

    private static void createWorkers(int numberOfWorkersToCreate) {
        System.out.println("Manager: creating "+ numberOfWorkersToCreate + "worker ec2 instances");
        try {
            for (int i = 0; i < numberOfWorkersToCreate; i++) {
                RunInstancesRequest runRequest = RunInstancesRequest.builder()
                        .imageId("ami-076515f20540e6e0b")
                        .instanceType(InstanceType.T2_MICRO)
                        .maxCount(1)
                        .minCount(1)
                        .userData(getWorkerData())
                        .build();
                RunInstancesResponse response = ec2.runInstances(runRequest);
                String instanceId = response.instances().get(0).instanceId();
                Tag tag = Tag.builder()
                        .key("role")
                        .value("worker")
                        .build();

                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                        .resources(instanceId)
                        .tags(tag)
                        .build();
                ec2.createTags(tagRequest);
            }
        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
