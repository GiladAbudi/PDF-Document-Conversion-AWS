Eilon Benami, ID:305336117. Gilad Abudi, ID:305544694
In order to run this project, you should create a jar containing all the classes, and run the jar using the following command:
java -jar <your jar file name> <input file name> <output file name> <number of lines per worker> (optional)<terminate>.

Explanation:
Once activated, the local application first checks if a running manager ec2 instance exists, and if it doesn't, creates it. 
then the local app uploads the input file to a bucket in s3, sends a message to a queue called appManagerQueue,
containing "New task", and the details about the bucket and the key for the input file. The manager is always listening
on the appManagerQueue for new tasks. The manager creates a threadpool using ExecutorService in order to add efficiency
(each application gets 2 threads - one that sends the input lines to the workers, and one that reads the output lines
from the workers).
once a new task message arrives, the manager takes the input file from the s3 link, and starts a new Runnable in order
to send the lines from the input file to the M2W queue, which is the queue between the manager to the worker. 
The manager counts the line in the input file, and creates the number of worker ec2 instances accordingly (if needed)
The workers receive messages from M2W queue, handle the task written in the line(convert pdf to image\html\text), and then upload it
to s3 bucket, and send a message in the W2M queue with the details for that bucket, and the application id they are working on.
A different thread in the threadpool of the manager handles the messages from W2M queue, and writes them to a new summary file.
After all the lines of a specific input file have been proccessed, the thread that is responsible for creating the summary
file uploads it to s3 buckets and sends it to the app on the managerAppQueue. If the manager received "terminate", it waits
for all the threads in the threadpool to finish, then terminates all the workers' instances, then finally terminates the
manager instance itself. 
After sending an input file, the app is always listening on managerAppQueue for done tasks. when a "done task" message
arrives, the app checks if the id given in that message is identical to the unique id the app has sent in the "new task"
message, and then takes the bucket and key from the message, downloads to summary file, and writes it to the 
<output file name> given as an argument.
Security: the credentials file is not sent on any scenario

Instance type: T2 Small. AMI: ami-076515f20540e6e0b (given in assignment requirements)
Time for first input file(2500 lines, 8 workers(max) using n = 100): 10 minutes
Time for 2nd input file(100 lines, 1 worker, using n=100, without a running manager): 7:30 minutes
Time for 2nd input file(100 lines, 8 workers(max), using n=10, with a running manager): 2 minutes


