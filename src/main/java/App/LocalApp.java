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

    public static void main(String[] args) {
        Region region = Region.US_EAST_1;
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();
        String linesPerWorker = "5";//args[3]
        String outputName = "output.txt";//args [2]
        String inputFile = "C:\\Users\\user1\\Desktop\\file.txt"; // args[1]
        boolean terminate = false; //args[4]
        String queueName = appManagerQueue;
        String appId = ""+ System.currentTimeMillis();
        String bucket = "bucket " + appId;
        String key = "key";
        createBucket(bucket);
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(inputFile)));

        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
           CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;
        }
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("New Task#"+bucket+"#"+key+"#"+linesPerWorker+"#"+appId)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
        try {
            Thread.sleep(5000);
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .build();
                List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
                String fileLink = "";
                for (Message m : messages) {
                    String body = m.body();
                    if (body.contains("Done task")) {
                        fileLink = "https://" + bucket + ".s3.amazonaws.com/" + key;
                    }
                }
            try (BufferedInputStream in = new BufferedInputStream(new URL(fileLink).openStream());
                 FileOutputStream fileOutputStream = new FileOutputStream(outputName)) {
                byte[] dataBuffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                    fileOutputStream.write(dataBuffer, 0, bytesRead);
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
        catch(InterruptedException e){
            System.out.println("got interrupted exception " + e.getMessage());
        }

    }
    private static void createBucket(String bucket) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .build());
    }
}