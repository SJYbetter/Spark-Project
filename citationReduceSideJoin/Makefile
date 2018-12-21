hadoop.root=/Users/jieyusheng/hadoop-2.8.4
spark.root=/Users/jieyusheng/spark-2.3.1-bin-hadoop2.7
spark.submit=$(spark.root)/bin/spark-submit

java_files = $(shell find src -type f -name "*.java")
scala_files = $(shell find src -type f -name "*.scala")

ifdef BIG_MEM
spark_submit_args= --driver-memory 2G --executor-memory 2G
endif

job.main=ReduceSideJoin
jar.name=jieyusheng-1.0-SNAPSHOT.jar

local.jar=target/${jar.name}
#local.args= string ../BIGDATA/DBLP_V8.txt
local.args= uuid ../BIGDATA/dblp-ref/dblp-ref-*.json
#local.args= int ../BIGDATA/DBLP_V5.txt
#local.args='/Users/jieyusheng/BIGDATA/dblp-ref/dblp-ref-*.json'

#ts=$(shell date +%m%d_%H%M%S)
aws.bucket.name=twittertriangle
aws.emr.release=emr-5.17.0
aws.instance.type=m4.xlarge

aws.log.dir=s3://${aws.bucket.name}/logs
aws.jar=s3://${aws.bucket.name}/$(jar.name)
aws.input=s3://${aws.bucket.name}/dblp-ref/dblp-ref-*.json
aws.output=s3://${aws.bucket.name}/final

aws.args="$(aws.input)","$(aws.output)","1","false"

all:  spark_local

$(local.jar): $(java_files) $(scala_files)
	mvn package -DskipTests=true

jar: $(local.jar)

clean_local_output:
	@rm -rf $(or ${local.output}, __output_not_defined__)

# ************************** local & pseudo spark target ********************************************
# local spark
spark_local: clean_local_output $(local.jar)
	${spark.submit} ${spark_submit_args} --class ${job.main} --master ${or ${spark.local}, local[*]} ${local.jar} ${local.args}

# remote spark
spark_pseudo: clean_local_output $(local.jar)
	@test -z "$(spark.pseudo)" && echo "error: spark.remote not defined" && false
	${spark.submit} ${spark_submit_args} --class ${job.main} --master ${spark.remote} --deploy-mode cluster ${local.jar} ${local.args}


# ************************** local & pseudo spark target ********************************************
upload-app-aws: ${local.jar}
	aws s3 cp $(local.jar) s3://${aws.bucket.name}

aws_cluster:
	aws emr create-cluster \
		--name "cs6240 final project" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":5,"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
		--ec2-attributes KeyName=u1 \
		--applications Name=Hadoop Name=Spark\
		--log-uri $(aws.log.dir)\
		--use-default-roles \
		--enable-debugging

aws_job:
	aws s3 rm ${aws.output} --recursive
	aws emr add-steps --cluster-id j-PWSX0LRFCEZH \
    	--steps Type=CUSTOM_JAR,Name=runit,ActionOnFailure=CONTINUE,Jar="command-runner.jar",Args=["spark-submit","--deploy-mode","cluster","--class","${job.main}","$(aws.jar)","$(aws.input)","${aws.output}"]


