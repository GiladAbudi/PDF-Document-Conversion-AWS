package Actors;

public class AppHandler implements Runnable {
    int counter;
    String workerQ,appId,bucket,key,appQueue;
    public AppHandler(int linesCounter, String workerOQUrl, String appId, String bucket, String key, String appQueueUrl) {
        counter = linesCounter;
        workerQ=workerOQUrl;
        this.appId=appId;
        this.bucket=bucket;
        this.key=key;
        this.appQueue=appQueueUrl;
    }
    public void run(){
        Manager.handleWorkersOutput(counter, workerQ, appId, bucket, key, appQueue);
    }
}
