package Actors;

import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.fit.pdfdom.PDFDomTree;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.imageio.ImageIO;
import javax.xml.parsers.ParserConfigurationException;
import java.awt.image.BufferedImage;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.List;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.Scanner;

public class Worker {
    private static S3Client s3;
    private static String bucket;

    public static void main(String[] args) throws Exception {

        String queueNameWorker2Manager = "W2M";
        String queueNameManager2Worker = "M2W";

        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        bucket = "bucket1586960757979w";//"bucket" + System.currentTimeMillis();

        //s3 instance
        s3 = S3Client.builder().region(Region.US_EAST_1).build();
        //createBucket(bucket);

        // create queue
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueNameWorker2Manager)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;
        }

        // get queue URL
        GetQueueUrlRequest getQueueRequestM2W = GetQueueUrlRequest.builder()
                .queueName(queueNameManager2Worker)
                .build();
        String queueUrlM2W = sqs.getQueueUrl(getQueueRequestM2W).queueUrl();

        GetQueueUrlRequest getQueueRequestW2M = GetQueueUrlRequest.builder()
                .queueName(queueNameWorker2Manager)
                .build();
        String queueUrlW2M = sqs.getQueueUrl(getQueueRequestW2M).queueUrl();


        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrlM2W)
                .maxNumberOfMessages(3)
                .visibilityTimeout(120)
                .build();


        // wait for msg from manager
        while (true) {
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message m : messages) {
                String msg = m.body();                              // appId # msg
                String input[] = msg.split("#",2);
                processMessage(sqs, queueUrlW2M, input[1] ,input[0]);
                System.out.println("Finish to process Message");
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrlM2W)
                        .receiptHandle(m.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteRequest);
                System.out.println("Delete the Message from SQS");
            }
        }

    }

////////////////////////////////////////////////////////////////////////////////////////////////


    private static void createBucket(String bucket) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .build());
    }


    private static void processMessage(SqsClient sqs, String queueUrl, String msg, String appId) throws Exception {
        String output = "";
        try {
            Scanner scanner = new Scanner(msg);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                System.out.println(line);
                output += ActionOnPDFfile(line,appId);
                // process the line
            }
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(output);
        sendOutputFile(output,appId, queueUrl, sqs);
    }

    private static String generateJPGFromPDF(String filename, String outputFileName, String appId) throws IOException {
        PDDocument pd = null;
        try {
            pd = PDDocument.load(new File(filename));
        } catch (IOException e) {
            return "cant-load-PDF";
        }

        PDFRenderer pr = new PDFRenderer(fisrtPageOfPDFfile(pd));
        BufferedImage bi = pr.renderImageWithDPI(0, 300);
        ImageIO.write(bi, "JPG", new File(outputFileName + ".jpg"));
        pd.close();
        try {
            uploadFileToS3(appId+outputFileName, ".jpg");
        } catch (Exception e) {
            return "cantUploadFile";
        }
        removeFile(outputFileName+".jpg");
        removeFile(outputFileName+".pdf");
        System.out.println("WORK JPG");
        return "https://" + bucket + ".s3.amazonaws.com/" + appId+outputFileName + ".jpg";
    }


    //TODO -FIX THROW EXCEPTION
    private static String generateHTMLFromPDF(String filename, String outputFileName, String appId) throws IOException {
        PDDocument pdf = null;
        try {
            pdf = PDDocument.load(new File(filename));
        } catch (IOException e) {
            return "cant-load-PDF";
        }
        PDDocument pdf1 = fisrtPageOfPDFfile(pdf);
        Writer output = new PrintWriter(outputFileName + ".html", "utf-8");

        try {

            new PDFDomTree().writeText(pdf1, output);
            uploadFileToS3(appId+outputFileName, ".html");
        } catch (ParserConfigurationException e) {
            return "cant-generateHTML";
        } catch (Exception e) {
            return "cantUploadFile";
        }
        pdf.close();
        pdf1.close();
        output.close();
        removeFile(outputFileName+".html");
        removeFile(outputFileName+".pdf");
        System.out.println("WORK HTML");
        return "https://" + bucket + ".s3.amazonaws.com/" + appId+outputFileName + ".html";
    }

    private static String generateTEXTFromPDF(String filename, String outputFileName, String appId) throws IOException {
        File f = new File(filename);
        String parsedText;
        PDFParser parser = new PDFParser(new RandomAccessFile(f, "r"));
        try {
            parser.parse();
        } catch (IOException e) {

            return "cant-parse-to-text";
        }
        COSDocument cosDoc = parser.getDocument();
        PDFTextStripper pdfStripper = new PDFTextStripper();
        PDDocument pdDoc = new PDDocument(cosDoc);
        parsedText = pdfStripper.getText(fisrtPageOfPDFfile(pdDoc));
        PrintWriter pw = new PrintWriter(outputFileName + ".txt");
        pw.print(parsedText);
        //upload the file to S3
        try {
            uploadFileToS3(appId+outputFileName, ".txt");
        } catch (Exception e) {
            return "cantUploadFile";
        }
        pdDoc.close();
        pw.close();
        removeFile(outputFileName+".txt");
        removeFile(outputFileName+".pdf");
        System.out.println("WORK TEXT");
        return "https://" + bucket + ".s3.amazonaws.com/" + appId+outputFileName + ".txt";
    }


    private static String downloadPDF(String fileURL, String fileName) throws IOException {
        URL url = new URL(fileURL);
        InputStream in = null;
        try {
            in = url.openStream();
            Files.copy(in, Paths.get(fileName), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            return "cant-access-to-file";
        }
        System.out.println("WORK download");
        return "";
    }

    private static PDDocument fisrtPageOfPDFfile(PDDocument pdfFile) throws IOException {
        PDDocument firstPage = new PDDocument();
        firstPage.addPage((PDPage) pdfFile.getPage(0));
        return firstPage;
    }

    private static String ActionOnPDFfile(String line,String appId) throws Exception {
        String res = "";
        String[] pharseline = line.split("\t");
        String action = pharseline[0];
        String url = pharseline[1];
        String[] slashParse = url.split("/");
        String fileName = "output/" + slashParse[slashParse.length - 1];      // output/___.pdf
        String name = fileName.split("\\.")[0];                    // output/___

        res = downloadPDF(url, fileName);     //download the pdf
        if (res.equals("") && action.equals("ToImage"))
            res = generateJPGFromPDF(fileName, name, appId);
        else if (res.equals("") && action.equals("ToHTML"))
            res = generateHTMLFromPDF(fileName, name, appId);
        else if (res.equals("") && action.equals("ToText"))
            res = generateTEXTFromPDF(fileName, name, appId);

        return action + '\t' + url + '\t' + res + '\n';

    }

    private static void sendOutputFile(String output,String appId, String queueUrl, SqsClient sqs) {
        output= "PDF task done#"+appId+"#"+output;
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(output)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
        System.out.println("Finish send msg to manager");
    }

    private static void uploadFileToS3(String outputFileName, String type) throws Exception {
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(outputFileName + type).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(outputFileName + type)));

    }

    private static void removeFile (String fileName) {
        File f = new File(fileName);
        f.delete();
    }
}
