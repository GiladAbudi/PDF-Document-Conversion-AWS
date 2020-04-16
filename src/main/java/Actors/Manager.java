package Actors;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {
    private static S3Client s3;
    private static SqsClient sqs;
    private static final String appManagerQueue = "appManagerQueue";
    private static final String workerIQ = "M2W";
    private static final String workerOQ = "W2M";


    public static void main(String args[]) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        Region region = Region.US_EAST_1;
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(appManagerQueue)
                .build();
        String appQueueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(workerIQ)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
            CreateQueueRequest request2 = CreateQueueRequest.builder()
                    .queueName(workerOQ)
                    .build();
            CreateQueueResponse create_result2 = sqs.createQueue(request2);

        } catch (QueueNameExistsException e) {
            throw e;
        }
        GetQueueUrlRequest getQueueRequest1 = GetQueueUrlRequest.builder()
                .queueName(workerIQ)
                .build();
        GetQueueUrlRequest getQueueRequest2 = GetQueueUrlRequest.builder()
                .queueName(workerOQ)
                .build();
        String workerIQUrl = sqs.getQueueUrl(getQueueRequest1).queueUrl();
        String workerOQUrl = sqs.getQueueUrl(getQueueRequest2).queueUrl();
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
                if (body.contains("New Task")) {
                    String[] split = body.split("#");
                    bucket = split[1];
                    key = split[2];
                    linesPerWorker = Integer.parseInt(split[3]);
                    appId = split[4];
                    if (split.length > 5) {
                        terminate = true;
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
                            // read next line
                            line = reader.readLine();
                        }
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(appQueueUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteRequest);
                    AppHandler handler = new AppHandler(linesCounter, workerOQUrl, appId, bucket, key, appQueueUrl);
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
                        terminate=true;
                        break;
                    }
                }
            }
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
    }


    private static void writeLineToOutput(String line, String outputFile) {
        try
        {
            FileWriter fw = new FileWriter(outputFile,true); //the true will append the new data
            fw.write(line);//appends the string to the file
            fw.close();
        }
        catch(IOException ioe)
        {
            System.err.println("IOException: " + ioe.getMessage());
        }
    }
}
