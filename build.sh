#!/bin/bash
set -x # expands variables and prints a little + sign before the line
export bucket_name
export version
export branch
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
export AWS_DEFAULT_REGION=us-east-1

echo "clean up m2 repository to avoid overrides and to reflect newchanges"
rm -rf ~/.m2/repository/com/apptium/eportal/"$bucket_name"
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
	#mvn install:install-file -Dfile=/var/lib/jenkins/Oracle/ojdbc8.jar -DgroupId=com.oracle -DartifactId=ojdbc8 -Dversion=12.2.0 -Dpackaging=jar
	#exitstatus
    mvn clean install -DskipTests
    exitstatus
}

ArtifactDeployment(){
	echo "************ starting s3 Artifact Deployment ************"
	aws s3 cp **/"$bucket_name"-"$version"."$zipas" s3://"$bucket_name"/"$artifact_folder"/JenkinsBuildId-"#"${BUILD_ID}/
	exitstatus
}

mavenbuild
ArtifactDeployment

# echo $?
# if [ $? == 0 ]; then
# 	echo "************ starting to build docker image ************"
# 	sudo docker build -t apptium/eportal-workflow-engine-1.0.0.jar .
# else
# 	echo "************ docker build failure :( ************"
# 	exit 1
# fi


