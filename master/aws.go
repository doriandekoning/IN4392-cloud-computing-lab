package main

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
)

func StartNewWorker() ([]*ec2.Instance, error) {
	var instances []*ec2.Instance
	svc := ec2.New(Sess)
	response, err := svc.RunInstances(&ec2.RunInstancesInput{
		LaunchTemplate: &ec2.LaunchTemplateSpecification{LaunchTemplateName: aws.String("worker")},
		MinCount:       aws.Int64(1),
		MaxCount:       aws.Int64(1),
	})

	if err != nil {
		log.Println("Could not create instance", err)
	} else {
		for _, inst := range response.Instances {
			instances = append(instances, inst)
		}

	}
	return instances, err
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
