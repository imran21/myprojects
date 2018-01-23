#!/bin/bash
set -x # expands variables and prints a little + sign before the line

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
export AWS_DEFAULT_REGION=us-east-1

echo "clean up m2 repository to avoid overrides and to reflect newchanges"
rm -rf ~/.m2/repository/com/apptium/eportal/eportal-eventstore
ls ~/.m2/repository/com/apptium/eportal/

exitstatus(){
	build="$?"
	if [ "$build" = 0 ];then
		echo "************ Build Successful :) ************"
  else
  	echo "************ Build Failure :( ************"
  	exit 1
  fi
}

cloverrun(){
	mvn clean clover:setup test clover:clover clover2:check -Dmaven.clover.targetPercentage=50% -Dmaven.clover.failOnViolation=false clover:log
	exitstatus
}

mavenbuild(){
	echo "************ starting to build maven project ************"
	mvn dependency:tree
	mvn clean install -DskipTests=True
	exitstatus
}

ArtifactDeployment(){
	echo "************ starting s3 Artifact Deployment ************"
	aws s3 cp **/eportal-eventstore-"$version".jar s3://eportal-eventstore/develop-builds/JenkinsBuildId-"#"${BUILD_ID}/
	exitstatus
}

mavenbuild
ArtifactDeployment
# echo $?
# if [ $? == 0 ]; then
# 	echo "************ starting to build docker image ************"
# 	sudo docker build -t apptium/eportal-workflow-engine-1.0.0 .
# else
# 	echo "************ docker build failued :( ************"
# 	exit 1
# fi
