package App;

import Actors.Manager;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.ec2.Ec2Client;

import javax.xml.bind.SchemaOutputResolver;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;

import static Actors.Manager.createQueue;


public class LocalApp {
    private static Ec2Client ec2;
    private static S3Client s3;
    private static SqsClient sqs;
    private static final String appManagerQueue = "appManagerQueue";
    private static final String managerAppQueue = "managerAppQueue";
    private static final String managerJar = "https://appbucket305336117.s3.amazonaws.com/manager.jar";
    private static final String managerAmi = "ami-076515f20540e6e0b";

    public static void main(String[] args) {
        Region region = Region.US_EAST_1;
        ec2= Ec2Client.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();
        String linesPerWorker = "1";//args[3]
        String outputName = "output.html";//args [2]
        String inputFile = "input-sample-1.txt"; // args[1]
        boolean terminate = false; //args[4]
        String appId = ""+ System.currentTimeMillis();
        String key = appId+inputFile;
        String bucket = "bucket1586960757978l";
        if(!isRunning("Manager")){
            createManager();
        }
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(inputFile)));

        String queueOutUrl = createQueue(appManagerQueue,sqs);
        String queueInUrl = createQueue(managerAppQueue,sqs);
        String toTerminate = "";
        if(terminate) {
            toTerminate = "#true";
        }
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueOutUrl)
                .messageBody("New Task#" + bucket + "#" + key + "#" + linesPerWorker + "#" + appId + toTerminate)
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

    private static boolean isRunning(String role){
        System.out.println("Checking if there are runnning or pending " +role+ " instances");
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
                                    return true;
                                }
                            }
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e) {
            e.getStackTrace();
        }
        return false;
    }

    private static void createManager() {
        try {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(managerAmi)
                    .instanceType(InstanceType.T2_SMALL)
                    .maxCount(1)
                    .minCount(1)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::577569430471:instance-profile/managerRole").build())
                    .securityGroups("eilon")
                    .keyName("eilon")
                    .userData(getManagerData())
                    .build();
            RunInstancesResponse response = ec2.runInstances(runRequest);
            String instanceId = response.instances().get(0).instanceId();
            Tag tag = Tag.builder()
                    .key("role")
                    .value("Manager")
                    .build();
            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();
            ec2.createTags(tagRequest);
        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
    private static String getManagerData() {
        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash");
        lines.add("cd home/ec2-user/");
        lines.add("wget " + managerJar + " -O manager.jar");
        lines.add("java -Xmx30g -jar manager.jar &> log.txt");
        return new String(Base64.getEncoder().encode(Manager.join(lines, "\n").getBytes()));
    }
}