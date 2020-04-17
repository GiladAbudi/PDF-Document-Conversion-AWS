package App;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.xml.bind.SchemaOutputResolver;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;


public class LocalApp {
    private static S3Client s3;
    private static SqsClient sqs;
    private static final String appManagerQueue = "appManagerQueue";
    private static final String managerAppQueue = "managerAppQueue";
    private static String bucket = "bucket1586960757979l";
    public static void main(String[] args) {
        Region region = Region.US_EAST_1;
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();
        String linesPerWorker = "5";//args[3]
        String outputName = "output.html";//args [2]
        String inputFile = "file.txt"; // args[1]
        boolean terminate = false; //args[4]
        String queueNameOut = appManagerQueue;
        String queueNameIn = managerAppQueue;
        String appId = ""+ System.currentTimeMillis();
        String key = appId+inputFile;
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(inputFile)));

        String queueOutUrl = createQueue(queueNameOut);
        String queueInUrl = createQueue(queueNameIn);
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueOutUrl)
                .messageBody("New Task#" + bucket + "#" + key + "#" + linesPerWorker + "#" + appId)
                .delaySeconds(5)
                .build();
        boolean done = false;
        sqs.sendMessage(send_msg_request);
        System.out.println("sent msg to manager");
        while (!done) {
            try {
                Thread.sleep(1000);
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueInUrl)
                        .build();
                String fileLink = "";
                List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

                for (Message m : messages) {
                    String body = m.body();
                    if (body.contains("Done task")) {
                        System.out.println("got done task, msg :"+body);
                        String[] split = body.split("#", 2);
                        if (split[1].equals(appId)) {
                            done = true;
                            System.out.println("done = true");
                            fileLink = "https://" + bucket + ".s3.amazonaws.com/output" + appId+".html";
                            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                                    .queueUrl(queueInUrl)
                                    .receiptHandle(m.receiptHandle())
                                    .build();
                            sqs.deleteMessage(deleteRequest);
                            if (!fileLink.equals("")){
                                try (BufferedInputStream in = new BufferedInputStream(new URL(fileLink).openStream());

                                     FileOutputStream fileOutputStream = new FileOutputStream(outputName)) {
                                    byte[] dataBuffer = new byte[1024];
                                    int bytesRead;
                                    while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                                        fileOutputStream.write(dataBuffer, 0, bytesRead);
                                    }
                                    sqs.deleteMessage(deleteRequest);
                                    System.out.println("output link: "+fileLink);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                                break;
                            }

                            try {
                                File output = new File(outputName);
                                if (!output.exists()) {
                                    output.createNewFile();
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }



                        }
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("got interrupted exception " + e.getMessage());
            }
        }
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

    private static void createBucket(String bucket) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .build());
    }
}