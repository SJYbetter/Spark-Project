hadoop.root=/opt/hadoop
jar.name=MRUploadHbase-1.0.jar
jar.path=target/${jar.name}
job.name=MRUploadHbaseMain

local.input=input
local.input.type=txt
local.output=output
local.config=hbaseconfig.json
local.params=${local.input} ${local.input.type} ${local.output}


# Pseudo-Cluster Execution
hdfs.user.name=moo
hdfs.input=input
hdfs.output=output
hdfs.params=${hdfs.k} ${hdfs.total} ${hdfs.input} ${hdfs.tmp} ${hdfs.output}

# AWS EMR Execution
aws.input=citationv10_sample
aws.input.type=json
aws.output=output

aws.emr.release=emr-5.19.0
aws.region=us-east-1
aws.bucket.name=pdpmapreduce6240
aws.subnet.id=subnet-aedea7a1
aws.num.nodes=5
aws.recordpath=bighero_mr_uploadhbase
aws.name=Big Hero MR Upload Hbase
aws.output=bighero/output/${aws.recordpath}
aws.log.dir=bighero/log/${aws.recordpath}
aws.instance.type=m4.xlarge

aws.params="s3://${aws.bucket.name}/${aws.input}","${aws.input.type}","${aws.output}"
# -----------------------------------------------make------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local: clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.params}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs:
	${hadoop.root}/sbin/stop-dfs.sh

# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.tmp}*

clean-hdfs-tmp:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.tmp}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.params}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: clean-local-output clean-hdfs-output clean-hdfs-tmp
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.params}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}

# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
aws: upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "${aws.name}" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}",${aws.params}],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--region ${aws.region} \
		--use-default-roles \
		--enable-debugging \
		--ec2-attributes SubnetId=${aws.subnet.id} \
		--auto-terminate \
		--configurations file://./${local.config}

aws-cli-report: delete-output-aws
	aws emr create-cluster \
	--applications Name=Hadoop --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-aedea7a1","EmrManagedSlaveSecurityGroup":"sg-0cc96ab1722d890e5","EmrManagedMasterSecurityGroup":"sg-0f31bae5896074618"}' \
	--release-label ${aws.emr.release} \
	--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
	--steps '[{"Args":["MRUploadHbaseMain","s3://pdpmapreduce6240/citationv10_sample","json","bighero/output/bighero_mr_uploadhbase"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"s3://pdpmapreduce6240/MRUploadHbase-1.0.jar","Properties":"","Name":"Custom JAR"}]' \
	--instance-groups '[{"InstanceCount":${aws.num.nodes},"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m4.xlarge","Name":"CORE"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m4.xlarge","Name":"MASTER"}]' \
	--configurations '[{"Classification":"hbase-site","Properties":{"hbase.rootdir":"s3://pdpmapreduce6240/MyHBaseStore_v10sample/"},"Configurations":[]},{"Classification":"hbase","Properties":{"hbase.emr.storageMode":"s3"},"Configurations":[]}]' \
	--auto-terminate \
	--service-role EMR_DefaultRole
	--enable-debugging \
	--name "${aws.name}" \
	--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
	--region us-east-1

aws-add-steps:
		aws emr add-steps \
			--cluster-id j-213KXY4C84DH1 \
			--steps '[{"Args":["MRUploadHbaseMain","s3://pdpmapreduce6240/${aws.input}","json","bighero/output/bighero_mr_uploadhbase"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"s3://pdpmapreduce6240/MRUploadHbase-1.0.jar","Properties":"","Name":"Custom JAR"}]'

aws-existing-jar: delete-output-aws
	aws emr create-cluster \
		--name "${aws.name}" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}",${aws.params}],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--region ${aws.region} \
		--use-default-roles \
		--enable-debugging \
		--ec2-attributes SubnetId=${aws.subnet.id} \
		--auto-terminate \
		--configurations file://./${local.config}

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f MR-Demo.tar.gz
	rm -f MR-Demo.zip
	rm -rf build
	mkdir -p build/deliv/MR-Demo
	cp -r src build/deliv/MR-Demo
	cp -r config build/deliv/MR-Demo
	cp -r input build/deliv/MR-Demo
	cp pom.xml build/deliv/MR-Demo
	cp Makefile build/deliv/MR-Demo
	cp README.txt build/deliv/MR-Demo
	tar -czf MR-Demo.tar.gz -C build/deliv MR-Demo
	cd build/deliv && zip -rq ../../MR-Demo.zip MR-Demo
