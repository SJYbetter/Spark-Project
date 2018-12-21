spark.root=/opt/spark
hadoop.root=/opt/hadoop
app.name=UploadHbaseMain
jar.dir=scala-2.11
fat.jar.name=UploadHbase-assembly-0.1.jar
fat.jar.path=target/${jar.dir}/${fat.jar.name}
jar.name=uploadhbase_2.11-0.1.jar
jar.path=target/${jar.dir}/${jar.name}
job.name=UploadHbaseMain

# local
local.master=local[4]
local.config=hbaseconfig.json
local.input=input
local.output=output

# aws
aws.input=citationv10
aws.input.type=json

aws.emr.release=emr-5.19.0
aws.region=us-east-1
aws.bucket.name=pdpmapreduce6240
aws.subnet.id=subnet-aedea7a1
aws.num.nodes=5
aws.recordpath=bighero_uploadhbase
aws.name=Big Hero upload hbase v8
aws.output=bighero/output/${aws.recordpath}
aws.log.dir=bighero/log/${aws.recordpath}
aws.instance.type=m4.xlarge

aws.params="s3://${aws.bucket.name}/${aws.input}","${aws.input.type}"

#---------------------make-----------------------

fat-jar:
	rm -rf target/*
	sbt assembly

jar:
	rm -rf target/*
	sbt package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*
	rm -rf ${local.result}*

# Runs standalone
local: clean-local-output
	spark-submit --class ${job.name} --master ${local.master} --name "${app.name}" ${jar.path} ${local.input} ${local.output} ${local.result} ${use.max.filter} ${max.value}

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

# Download output from HDFS to local.
download-output-hdfs:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output
	spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: clean-local-output clean-hdfs-output
	#spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload hbase config to S3
upload-hbaseconfig-aws: make-bucket
	aws s3 cp ${local.config} s3://${aws.bucket.name}/

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}

# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${fat.jar.path} s3://${aws.bucket.name}

# Main EMR launch.
aws: upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "${aws.name}" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark Name=HBase \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${fat.jar.name}",${aws.params}] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--ec2-attributes SubnetId=${aws.subnet.id} \
		--auto-terminate \
		--configurations file://./${local.config}

aws-existing-jar: delete-output-aws
	aws emr create-cluster \
		--name "${aws.name}" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark Name=HBase \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${fat.jar.name}",${aws.params}] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
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
	rm -f Spark-Demo.tar.gz
	rm -f Spark-Demo.zip
	rm -rf build
	mkdir -p build/deliv/Spark-Demo
	cp -r src build/deliv/Spark-Demo
	cp -r config build/deliv/Spark-Demo
	cp -r input build/deliv/Spark-Demo
	cp pom.xml build/deliv/Spark-Demo
	cp Makefile build/deliv/Spark-Demo
	cp README.txt build/deliv/Spark-Demo
	tar -czf Spark-Demo.tar.gz -C build/deliv Spark-Demo
	cd build/deliv && zip -rq ../../Spark-Demo.zip Spark-Demo