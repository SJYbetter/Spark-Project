aws.bucket.name=pdpmapreduce6240
aws.input=citationv10
aws.input.type=json
aws.num.nodes=5
aws.recordpath=bighero_${aws.input}_${aws.num.nodes}
aws.hbase.store=MyHBaseStorage_${aws.input}_2
aws.log.dir=bighero/log
aws.instance.type=m4.xlarge

upload.job.name=MRUploadHbaseMain
upload.tmp=bighero/output
upload.params="${upload.job.name}","s3://${aws.bucket.name}/${aws.input}","${aws.input.type}","${upload.tmp}"

join.app.name=HBaseJoinMain1
aws.output=bighero/output/${aws.recordpath}_${join.app.name}
join.output=${aws.output}
join.cache.size=1000
join.partition.cnt=300
join.params="s3://${aws.bucket.name}/${join.output}","${join.cache.size}","${join.partition.cnt}"

delete-hbase-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.hbase.store}"

delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}"

aws-add-steps-upload:
	aws emr add-steps \
		--cluster-id j-3SVU8BO2HXQOE \
		--steps '[{"Args":[${upload.params}],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"s3://pdpmapreduce6240/MRUploadHbase-1.0.jar","Properties":"","Name":"Custom JAR"}]'

aws-add-steps-join: delete-output-aws
	aws emr add-steps \
		--cluster-id j-3N7AQQBCOK1PJ \
		--steps Type=CUSTOM_JAR,Name="${join.app.name}",Jar="command-runner.jar",ActionOnFailure=CONTINUE,Args=["spark-submit","--deploy-mode","cluster","--class","${join.app.name}","s3://${aws.bucket.name}/HBaseJoin-assembly-0.1.jar",${join.params}] \

aws: delete-hbase-aws delete-output-aws
	aws emr create-cluster \
		--applications Name=Hadoop Name=Spark Name=HBase \
		--ec2-attributes '{"KeyName":"CS6240","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-2baf6515","EmrManagedSlaveSecurityGroup":"sg-0cc96ab1722d890e5","EmrManagedMasterSecurityGroup":"sg-0f31bae5896074618"}' \
		--release-label emr-5.19.0 \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--steps '[{"Args":[${upload.params}],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://pdpmapreduce6240/MRUploadHbase-1.0.jar","Properties":"","Name":"Custom JAR"},{"Args":["spark-submit","--deploy-mode","cluster","--deploy-mode","cluster","--class","${join.app.name}","s3://pdpmapreduce6240/HBaseJoin-assembly-0.1.jar",${join.params}],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' \
		--instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}","Name":"Master - 1"},{"InstanceCount":${aws.num.nodes},"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}","Name":"Core - 2"}]' \
		--configurations '[{"Classification": "hadoop-env", "Properties": {"MALLOC_ARENA_MAX": "4"},"Configurations":[]},{"Classification":"yarn-site","Properties": {"yarn.nodemanager.vmem-check-enabled": "false"},"Configurations":[]},{"Classification":"hbase","Properties":{"hbase.emr.storageMode":"s3"},"Configurations":[]},{"Classification":"hbase-site","Properties":{"hbase.rootdir":"s3://pdpmapreduce6240/${aws.hbase.store}"},"Configurations":[]}]' \
		--auto-scaling-role EMR_AutoScaling_DefaultRole \
		--ebs-root-volume-size 10 \
		--service-role EMR_DefaultRole \
		--enable-debugging \
		--name 'BigHero Cluster v2' \
		--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
		--region us-east-1
