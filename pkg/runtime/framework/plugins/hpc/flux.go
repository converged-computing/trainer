/*
Copyright 2024 The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hpc

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/utils/ptr"

	"github.com/go-logr/logr"

	fluxapi "github.com/flux-framework/flux-operator/api/v1alpha2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	apiruntime "k8s.io/apimachinery/pkg/runtime"

	trainerapi "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"github.com/kubeflow/trainer/v2/pkg/runtime"
	"github.com/kubeflow/trainer/v2/pkg/runtime/framework"
)

// We can customize not easily exposed MiniCluster attributes with annotations

var (
	// the flux view image is the base OS / version for the view to install flux
	fluxViewImageAnnotation = "flux-framework.org/flux-operator.flux-view-image"

	// Disable using the view. E.g., only configuration is done.
	// This assumes that Flux is in the application container
	fluxViewDisableAnnotation = "flux-framework.org/flux-operator.disable-view"

	// working directory for now from annotation
	fluxWorkDirAnnotation = "flux-framework.org/flux-operator.working-dir"
)

// Interface assertions to ensure our struct implements the correct plugin interfaces at compile time.
var _ framework.ComponentBuilderPlugin = (*Flux)(nil)
var _ framework.TrainJobStatusPlugin = (*Flux)(nil)
var _ framework.WatchExtensionPlugin = (*Flux)(nil)

const Name = "flux-hpc"

// Flux implements the necessary plugin interfaces for a complete backend.
type Flux struct {
	client     client.Client
	restMapper meta.RESTMapper
	scheme     *apiruntime.Scheme
	logger     logr.Logger
}

// New is the factory function for the Flux plugin.
func New(_ context.Context, c client.Client, _ client.FieldIndexer) (framework.Plugin, error) {
	return &Flux{
		client:     c,
		restMapper: c.RESTMapper(),
		scheme:     c.Scheme(),
	}, nil
}

// Name returns the identifier for this plugin.
func (f *Flux) Name() string {
	return Name
}

// Build is the core method from ComponentBuilderPlugin. It replaces 'Create'.
// Its job is to construct and return all Kubernetes objects required by this backend.
func (f *Flux) Build(ctx context.Context, info *runtime.Info, trainJob *trainerapi.TrainJob) ([]any, error) {
	if info == nil || trainJob == nil {
		return nil, fmt.Errorf("runtime info or TrainJob object is missing")
	}

	// Do not update the MiniCluster if it already exists and is not suspended
	oldMc := &fluxapi.MiniCluster{}
	if err := f.client.Get(ctx, client.ObjectKeyFromObject(trainJob), oldMc); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		oldMc = nil
	}
	if oldMc != nil && !ptr.Deref(trainJob.Spec.Suspend, false) {
		return nil, nil
	}

	// Pull out trainer for easier access
	trainer := trainJob.Spec.Trainer

	// Assume if nodes invalid, give them one.
	nodes := *trainer.NumNodes
	if nodes <= int32(0) {
		nodes = int32(1)
		return nil, fmt.Errorf("trainer.spec.replicas must be a positive integer for the %s backend", f.Name)
	}

	// If no trainer image, set to a useful one.
	// 	https://www.kubeflow.org/docs/components/notebooks/container-images/
	image := "ghcr.io/kubeflow/kubeflow/notebook-servers/jupyter-pytorch-full"
	if *trainer.Image != "" {
		image = *trainer.Image
	}

	// Interactive if there is no command provided
	command := append(trainer.Command, trainer.Args...)
	interactive := len(command) == 0

	// Choices for init container
	// ghcr.io/converged-computing/flux-view-rocky:arm-9
	// ghcr.io/converged-computing/flux-view-rocky:arn-8
	// ghcr.io/converged-computing/flux-view-rocky:tag-9
	// ghcr.io/converged-computing/flux-view-rocky:tag-8
	// ghcr.io/converged-computing/flux-view-ubuntu:tag-noble
	// ghcr.io/converged-computing/flux-view-ubuntu:tag-jammy
	// ghcr.io/converged-computing/flux-view-ubuntu:tag-focal
	// ghcr.io/converged-computing/flux-view-ubuntu:arm-jammy
	// ghcr.io/converged-computing/flux-view-ubuntu:arm-focal
	// The user can change the init container via the trainjob init container.
	fluxViewImage, ok := trainJob.Annotations[fluxViewImageAnnotation]
	if !ok {
		fluxViewImage = "ghcr.io/converged-computing/flux-view-ubuntu:arm-jammy"
	}

	// Disable using the flux view?
	disableView := false
	_, ok = trainJob.Annotations[fluxViewDisableAnnotation]
	if ok {
		disableView = true
	}

	// TODO need to generate / map resources here
	resources := fluxapi.ContainerResources{}

	miniCluster := &fluxapi.MiniCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      trainJob.Name,
			Namespace: trainJob.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(trainJob, trainerapi.SchemeGroupVersion.WithKind("Trainer")),
			},
		},
		Spec: fluxapi.MiniClusterSpec{
			Size:        nodes,
			Tasks:       nodes,
			Interactive: interactive,
			Flux: fluxapi.FluxSpec{
				Container: fluxapi.FluxContainer{Image: fluxViewImage, Disable: disableView},
			},

			// Assume just one application container for now.
			Containers: []fluxapi.MiniClusterContainer{{
				Image:     image,
				Command:   strings.Join(command, " "),
				Resources: resources,
			}},
		},
	}

	// Add a working directory from an annotation
	fluxWorkDir, ok := trainJob.Annotations[fluxWorkDirAnnotation]
	if ok {
		miniCluster.Spec.Containers[0].WorkingDir = fluxWorkDir
	}

	// The method must return a slice of objects.
	return []any{miniCluster}, nil
}

// Status implements the TrainJobStatusPlugin interface. It reads the status
// of our MiniCluster and maps it to the TrainJob's status conditions.
func (f *Flux) Status(ctx context.Context, trainJob *trainerapi.TrainJob) (*trainerapi.TrainJobStatus, error) {
	miniCluster := &fluxapi.MiniCluster{}
	err := f.client.Get(ctx, client.ObjectKeyFromObject(trainJob), miniCluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the MiniCluster isn't found yet, the job is still pending.
			return nil, nil
		}
		return nil, err
	}
	// TODO need to figure out how to do this
	status := trainJob.Status.DeepCopy()
	var statuses []trainerapi.JobStatus
	status.JobsStatus = statuses
	return status, nil
}

// ReconcilerBuilders implements the WatchExtensionPlugin interface.
// It tells the main controller to also watch for MiniCluster events.
func (f *Flux) ReconcilerBuilders() []runtime.ReconcilerBuilder {
	return []runtime.ReconcilerBuilder{
		func(b *builder.Builder, cl client.Client, cache cache.Cache) *builder.Builder {
			return b.Watches(
				&fluxapi.MiniCluster{},
				handler.EnqueueRequestForOwner(
					f.scheme, f.client.RESTMapper(), &trainerapi.TrainJob{}, handler.OnlyControllerOwner(),
				),
			)
		},
	}
}
