package main

import (
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/s3"
)

func StartNewWorker() ([]*ec2.Instance, error) {
	if os.Getenv("LOCAL") != "" {
		return nil, nil
	}
	var instances []*ec2.Instance
	svc := ec2.New(Sess)
	response, err := svc.RunInstances(&ec2.RunInstancesInput{
		LaunchTemplate: &ec2.LaunchTemplateSpecification{LaunchTemplateName: aws.String("worker")},
		MinCount:       aws.Int64(1),
		MaxCount:       aws.Int64(1),
	})

	if err != nil {
		log.Println("Could not create instance", err)
		return nil, err
	}

	for _, inst := range response.Instances {
		instances = append(instances, inst)
	}

	return instances, nil
}

func StartWorkers(workers []*worker) error {
	svc := ec2.New(Sess)
	_, err := svc.StartInstances(&ec2.StartInstancesInput{
		InstanceIds: getInstanceIds(workers),
	})

	if err != nil {
		log.Println("Could not start instances", err)
		return err
	}
	return nil
}

func StopWorkers(workers []*worker) error {
	svc := ec2.New(Sess)
	_, err := svc.StopInstances(&ec2.StopInstancesInput{
		InstanceIds: getInstanceIds(workers),
	})

	if err != nil {
		log.Println("Could not stop instances", err)
		return err
	}
	return nil
}

func TerminateWorkers(workers []*worker) error {
	var instanceIds = getInstanceIds(workers)
	if len(instanceIds) < 1 {
		return nil
	}
	svc := ec2.New(Sess)
	_, err := svc.TerminateInstances(&ec2.TerminateInstancesInput{
		InstanceIds: instanceIds,
	})

	if err != nil {
		log.Println("Could not terminate instances", err)
		return err
	}
	return nil
}

func getInstanceIds([]*worker) []*string {
	var instanceIds []*string
	for _, worker := range workers {
		if worker.InstanceId != "" {
			instanceIds = append(instanceIds, &worker.InstanceId)
		}
	}
	return instanceIds
}

func PostMetrics(log *os.File, key string) error {
	scv := s3.New(Sess)
	_, err := scv.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("in4392metrics"),
		Key:    aws.String(key),
		Body:   log,
	})
	if err != nil {
		return err
	}
	return nil
}
