Replicating the Experiment Setup
================================

This guide provides instructions for setting up the experimental environment as described in the associated publication. By following these steps, you will create the necessary AWS infrastructure, configure network topologies (fog and cloud), apply traffic control settings, prepare Kubernetes (k3s) nodes with the necessary services (RabbitMQ and MongoDB), run dry runs and deployments according to produced schedules.

**Disclaimer:** This setup may incur AWS costs. Ensure you understand the pricing and properly clean up resources after the experiment. The instructions assume familiarity with the AWS CLI and basic Linux administration.

Prerequisites
-------------

1.  **AWS Account**: Ensure you have an active AWS account.
    
2.  **AWS CLI**: Install and configure the AWS CLI with programmatic access. [Installing the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
    
3.  **Key Pair**: Have an existing EC2 key pair (e.g., TestKeyPair) to SSH into instances.
    
4.  **Permissions**: Your IAM user/role should have permissions to create VPCs, subnets, EC2 instances, security groups, route tables, and internet gateways.
    

Step-by-Step Setup
------------------

### 1\. Create a VPC & Subnets

Create a VPC with a CIDR block of 10.0.0.0/16:
```sh
aws ec2 create-vpc --cidr-block 10.0.0.0/16
```

**Output Example:**

`{"Vpc": { "VpcId": "vpc-05c2d1f248dcdf47a", ...} }`

Record the VpcId, for example: vpc-05c2d1f248dcdf47a.

We will use the eu-west-1 region and corresponding Availability Zones.

### 2\. Fog Topology Setup

**Create Fog Subnet:**
```sh
aws ec2 create-subnet \    
    --vpc-id vpc-05c2d1f248dcdf47a \    
    --cidr-block 10.0.1.0/24 \    
    --availability-zone eu-west-1a
```

**Output Example:**

`   {      "Subnet": {          "SubnetId": "subnet-0c50b165f2732541d",          ...      }  }   `

Record the Fog Subnet ID: subnet-0c50b165f2732541d.

**Create Fog Security Group:**
```sh
aws ec2 create-security-group \    
    --group-name FogSG \    
    --description "Fog Security Group" \    
    --vpc-id vpc-05c2d1f248dcdf47a   
```

**Output Example:**

`   {      "GroupId": "sg-017da6fa4376d88fe"  }   `

Authorize inbound traffic within the group and SSH access:
```sh
aws ec2 authorize-security-group-ingress --group-id sg-017da6fa4376d88fe --protocol all --port all --source-group sg-017da6fa4376d88fe  
aws ec2 authorize-security-group-ingress --group-id sg-017da6fa4376d88fe --protocol tcp --port 22 --cidr 0.0.0.0/0 
```
**Launch Fog Instances (Example AMI: Ubuntu 64-bit):**
```sh
aws ec2 run-instances --image-id ami-0c38b837cd80f13bb --count 1 --instance-type i3.large   --key-name TestKeyPair --subnet-id subnet-0c50b165f2732541d --security-group-ids sg-017da6fa4376d88fe  
aws ec2 run-instances --image-id ami-0c38b837cd80f13bb --count 1 --instance-type t2.medium --key-name TestKeyPair --subnet-id subnet-0c50b165f2732541d --security-group-ids sg-017da6fa4376d88fe  
```

### 3\. Cloud Topology Setup

**Create Cloud Subnet:**
```sh
aws ec2 create-subnet \    
    --vpc-id vpc-05c2d1f248dcdf47a \    
    --cidr-block 10.0.2.0/24 \    
    --availability-zone eu-west-1a
```

Record Cloud Subnet ID: subnet-04304f5c094d6ba8d.

**Create Cloud Security Group:**
```sh
aws ec2 create-security-group \    
    --group-name CloudSG \    
    --description "Cloud Security Group" \    
    --vpc-id vpc-05c2d1f248dcdf47a
```

**Example Output:**

`   {      "GroupId": "sg-06d3307423e45a700"  }   `

Authorize inbound traffic:
```sh
aws ec2 authorize-security-group-ingress --group-id sg-06d3307423e45a700 --protocol all --port all --source-group sg-06d3307423e45a700  

aws ec2 authorize-security-group-ingress --group-id sg-06d3307423e45a700 --protocol tcp --port 22 --cidr 0.0.0.0/0
```

**Launch High-Capability Instances:**
```sh
aws ec2 run-instances --image-id ami-0c38b837cd80f13bb --count 2 --instance-type r5.xlarge   --key-name TestKeyPair --subnet-id subnet-04304f5c094d6ba8d --security-group-ids sg-06d3307423e45a700  
aws ec2 run-instances --image-id ami-0c38b837cd80f13bb --count 2 --instance-type r5.large    --key-name TestKeyPair --subnet-id subnet-04304f5c094d6ba8d --security-group-ids sg-06d3307423e45a700  
aws ec2 run-instances --image-id ami-0c38b837cd80f13bb --count 2 --instance-type m4.xlarge   --key-name TestKeyPair --subnet-id subnet-04304f5c094d6ba8d --security-group-ids sg-06d3307423e45a700  
aws ec2 run-instances --image-id ami-0c38b837cd80f13bb --count 2 --instance-type m4.2xlarge  --key-name TestKeyPair --subnet-id subnet-04304f5c094d6ba8d --security-group-ids sg-06d3307423e45a700 
```

### 4\. Routing Tables

**Create a Route Table:**
```sh
aws ec2 create-route-table --vpc-id vpc-05c2d1f248dcdf47a
```

**Associate Route Table with Subnets:**
```sh
aws ec2 associate-route-table --route-table-id rtb-072725b2d3305d50b --subnet-id subnet-0c50b165f2732541d  
aws ec2 associate-route-table --route-table-id rtb-072725b2d3305d50b --subnet-id subnet-04304f5c094d6ba8d   `
```
**Create and Attach Internet Gateway:**
```sh
aws ec2 create-internet-gateway  aws ec2 attach-internet-gateway --vpc-id vpc-05c2d1f248dcdf47a --internet-gateway-id igw-0f89a3b87eae240b2
```

**Create Default Route:**
```sh
aws ec2 create-route --route-table-id rtb-072725b2d3305d50b --destination-cidr-block 0.0.0.0/0 --gateway-id igw-0f89a3b87eae240b2
```

### 5\. Elastic IPs and Connectivity (Optional)

Attach Elastic IP addresses if needed and update your SSH configs accordingly.

### 6\. Simulate Network Conditions with tc

**Install tc** (usually pre-installed):
```sh
sudo apt-get update  sudo apt-get install -y iproute2
```
**Configure tc for Fog Instances** to simulate a 50 Mbps WAN link with 100 ms delay (adjust enX0 to your network interface, e.g. eth0):

```sh
sudo tc qdisc add dev enX0 root handle 1: htb default 12  
sudo tc class add dev enX0 parent 1: classid 1:1 htb rate 50mbit  
sudo tc qdisc add dev enX0 parent 1:1 handle 10: netem delay 100ms  
sudo tc filter add dev enX0 protocol ip parent 1:0 prio 1 u32 match ip dst 10.0.2.0/24 flowid 1:1
```
**Configure tc for Cloud Instances** similarly:
```sh
sudo tc qdisc add dev enX0 root handle 1: htb default 12  
sudo tc class add dev enX0 parent 1:1 htb rate 50mbit  
sudo tc qdisc add dev enX0 parent 1:1 handle 10: netem delay 100ms  
sudo tc filter add dev enX0 protocol ip parent 1:0 prio 1 u32 match ip dst 10.0.1.0/24 flowid 1:1
```

### 7\. Node Setup (Samba + SMBv3) *on each node*

**Install Samba:**
```sh
sudo apt-get update  
sudo apt-get install -y samba
```

**Set permissions for shared directory**:
```sh
sudo mkdir /out
sudo chown nobody:nogroup /out
sudo chmod 777 /out
```

**Configure Samba:** (edit /etc/samba/smb.conf)
```ini
[global]
    server min protocol = SMB3
    server max protocol = SMB3
[Share]
    path = /out
    browsable = yes
    writable = yes
    guest ok = yes
    read only = no
```

**Restart Samba:**
```sh
sudo systemctl restart smbd
```

**Configure Firewall:**
```sh
sudo ufw allow Samba
```

**Install CIFS Utils:**
```sh
sudo apt-get install -y cifs-utils
```

**Create Mount Points:**
```sh
for i in {1..2}; do    
    sudo mkdir -p /Fog$i    
    sudo chmod 777 /Fog$i  
done
for i in {1..8}; do    
    sudo mkdir -p /Cloud$i    
    sudo chmod 777 /Cloud$i  
done
```
**Mount Samba Shares** (adjust with node names):
```sh
sudo mount -t cifs -o guest,vers=3 //<NODE_HOSTNAME>/Share /<NODE_NAME>
```  
### Example
```sh
sudo mount -t cifs -o guest,vers=3 //cloud1/Share /Cloud1
```

### 8\. Disable Swap

On **all** nodes:
```sh
sudo swapoff -a  
sudo sed -i '/ swap / s/^/#/' /etc/fstab
```

### 9\. Set Hostnames

On each node, set its hostname (fog1/fog2/fog3/cloud1/...):
```sh
sudo hostnamectl set-hostname <hostname>
```

Update /etc/hosts if necessary.

### 10\. Install k3s

Follow the official k3s quick start guide: [https://docs.k3s.io/quick-start](https://docs.k3s.io/quick-start)

*   Install k3s on Fog1 (as the server node).
    
*   Join all other nodes as agents using the token from Fog1.
    

### 11\. Deploy Pipeline Services (RabbitMQ & MongoDB)

Create namespaces:
```sh
k3s kubectl create namespace mongodb  
k3s kubectl create namespace rabbitmq
```

Deploy RabbitMQ and MongoDB as per the provided deployments [`rabbitmq.yaml`](rabbitmq.yaml) and [`mongo.yaml`](mongo.yaml).

```sh
k3s kubectl apply -f rabbitmq.yaml -n rabbitmq
k3s kubectl apply -f mongo.yaml -n mongodb
```

### 12\. Prepare Cluster for Pipeline Execution

Copy all files from the folder **dry-runs** (provided with the repository) to `fog1` local file system (e.g., /home/ubuntu/).

Copy folder **samples** (provided with the repository) to `fog1` local file system (e.g., /home/ubuntu/). Additional samples can be made available on demand.

**Install yq on `fog1`**

Follow instructions here: [https://kislyuk.github.io/yq/](https://kislyuk.github.io/yq/)

**Set up dry run environment**

Set the `NODE_NAME` environment variable on *all nodes* to Fog1/Fog2/Cloud1/Cloud2/... respectively.

### Example

On Fog2 node:

```sh
export NODE_NAME=Fog2
```

You can add this line to the .bashrc or .profile file on each node to ensure the environment variable is set automatically on login:

```sh
echo 'export NODE_NAME=Fog2' >> ~/.bashrc
source ~/.bashrc
```
### 13\. Execute Dry Runs

Run script `00-start-dry-runs.sh` on Fog1 node from the folder you copied the repo contents.

```sh
./00-start-dry-runs.sh
```

**(Optional) Modifying Variables in Shell Script**

If your installation folders are different from the ones described in this README.md, you will need to modify the following variables within the shell script:

 - `input_dir`: Directory where the input .tar.bz2 files are located.
 - `out_dir`: Directory where the output files will be moved to.
 - `done_dir`: Directory where processed files will be moved to.

### Example
```sh
input_dir="/path/to/your/samples/"
out_dir="/path/to/your/out"
done_dir="/path/to/your/done"
```

**Running Deployment Time Tests (on all nodes)**
To test the deployment times of the Docker images, run the `00-test-deployment-times.sh` script on all nodes.
```sh
./00-test-deployment-times.sh
```
This script will:

 - Pull the Docker images for each pipeline step.
 - Measure the download and instance start times.
 - Log the metrics to a CSV file located at `/out/log/deployment_metrics.csv` in the `fog1` node.

Make sure the NODE_NAME environment variable is set correctly on each node before running the script.


**Dry Run Outputs**

Dry run execution metrics will be stored in the local folder on the Fog1 node `/out/log`. Dry run metrics are stored in three files: 

 - `step_performance_metrics.csv` - data about the recorded CPU and memory usage
 - `step_metrics.csv` - data about number/volume of inputs/outputs, step processing time and data transmission time (not used in analysis).
 - `deployment_metrics.csv` - provisioning and deployment times for the pipeline images used in the pipeline.

These dry run metrics are used by the CATS scheduler to estimate requirements and times of execution of steps at larger scales of deployment.

### 14\. Execute Schedule Deployment

Once a schedule has been created by the CATS scheduler, you can retrieve the schedule timeline serialization in CSV format from the `scheduler` base folder. The generated timelines will be saved as CSV files in the scheduler directory with filenames in the format:

`timeline_metrics3_{current_time}_deadline{deadline}_budget{budget}_input{input_volume_MB}MB_maxscalability{max_scalability}.csv`

Copy all files from the folder **deployments** (provided with the repository) and the generated timeline to `fog1` local file system (e.g., /home/ubuntu/).

Run script `01-run-deployment.sh` on Fog1 node from the folder you copied the repo contents. The script takes the CSV file name of a schedule timeline as input.

```sh
./01-run-deployment.sh <NAME_OF_CSV_FILE>
```

### Example
```sh
./01-run-deployment.sh timeline_metrics3_20241121_212353_deadline1000_budget100_input1229MB_maxscalability1.csv
```
**Schedule Deployment Outputs**

The 01-run-deployment.sh script records the following output:

 - `Total Time of Execution`: The total time taken to execute all the jobs in the schedule.

 - `Step Times`: The start time, end time, and total time for each step in the deployment.
The output is saved in two CSV files:

`deployment_times.csv`: This file contains the total time of execution for the entire deployment. It is located in the `/out/log` folder in the Fog1 node.
 - **dry_run_id**: The ID of the dry run.
deployment_csv: The name of the deployment CSV file.
 - **total_time**: The total time of execution in seconds.
 - **input_mbs**: The input size in MB.

`step_times.csv`: This file contains the timing information for each step in the deployment. It is also located in the `/out/log` folder in the Fog1 node.

 - **dry_run_id**: The ID of the dry run.
 - **job_name**: The name of the job.
 - **start_time**: The start time of the job (relative to the start of the first job).
 - **end_time**: The end time of the job.
 - **total_time**: The total time taken by the job.
 - **input_mbs**: The input size in MB.

Resource usage metrics and other details about the individual step execution are also recorded in the folder `/out/log` the same way as when executing the Dry Runs (see **Dry Run Outputs** section above).

Cleanup
-------

After the experiment, remember to:

*   Unmount SMB shares
    
*   Remove tc configurations
    
*   Terminate EC2 instances
    
*   Delete subnets, route tables, and VPC
    
*   Detach and delete the internet gateway
    
*   Delete any associated AWS resources to avoid ongoing costs.
    

Additional Notes
----------------

*   Adjust AMI IDs, instance types, and region specifics as necessary.
    
*   Network interfaces may differ in naming (e.g., eth0 instead of enX0).
    
*   Ensure that the configuration aligns with your security requirements (e.g., limiting SSH access, disabling guest mounts, etc.).
    
