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

	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	trainerapi "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"github.com/kubeflow/trainer/v2/pkg/constants"
	"github.com/kubeflow/trainer/v2/pkg/runtime"
	"github.com/kubeflow/trainer/v2/pkg/runtime/framework"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/jobset/client-go/applyconfiguration/jobset/v1alpha2"
)

// We can customize not easily exposed MiniCluster attributes with settings
var (
	// the flux view image is the base OS / version for the view to install flux
	fluxViewSetting   = "flux-view-image"
	fluxNetworkDevice = "flux-network-device"
	fluxQueuePolicy   = "flux-queue-policy"
	fluxInteractive   = "flux-interactive"

	// Defaults for the above
	defaultFluxView      = "ghcr.io/converged-computing/flux-view-ubuntu:arm-jammy"
	defaultNetworkDevice = "eth0"
	defaultQueuePolicy   = "fcfs"
	defaultInteractive   = "false"
)

var _ framework.CustomValidationPlugin = (*Flux)(nil)
var _ framework.ComponentBuilderPlugin = (*Flux)(nil)
var _ framework.EnforceMLPolicyPlugin = (*Flux)(nil)
var _ framework.WatchExtensionPlugin = (*Flux)(nil)

const Name = "Flux"

type Flux struct {
	client client.Client
	scheme *apiruntime.Scheme
}

func New(_ context.Context, client client.Client, _ client.FieldIndexer) (framework.Plugin, error) {
	return &Flux{
		client: client,
		scheme: client.Scheme(),
	}, nil
}

func (f *Flux) Name() string {
	return Name
}

func (f *Flux) Validate(_ context.Context, runtimeInfo *runtime.Info, _, newJobObj *trainerapi.TrainJob) (admission.Warnings, field.ErrorList) {
	var allErrs field.ErrorList
	if runtimeInfo == nil || runtimeInfo.RuntimePolicy.MLPolicySource == nil || runtimeInfo.RuntimePolicy.MLPolicySource.HPC == nil {
		return nil, allErrs
	}
	// We probably can do more validation here - not much to do for now.
	return nil, allErrs
}

// TODO: we likely want to move logic from Build up here, to mirror MPI plugin
func (f *Flux) EnforceMLPolicy(info *runtime.Info, trainJob *trainerapi.TrainJob) error {
	return nil
}

func (f *Flux) Build(ctx context.Context, info *runtime.Info, trainJob *trainerapi.TrainJob) ([]any, error) {

	// policy defines the Flux HPC cluster setup
	// Many configuration params cannot be represented in JobSet alone.
	policy := info.RuntimePolicy.MLPolicySource

	// Don't error, but assume this can't be applied here
	js, ok := runtime.TemplateSpecApply[v1alpha2.JobSetSpecApplyConfiguration](info)
	if !ok || js == nil {
		return nil, nil
	}

	// If the user's chosen runtime:
	// 1. Does not have the 'hpc' policy enabled
	// 2. Has not chosen flux... this plugin does nothing.
	if policy == nil || policy.HPC == nil || strings.ToLower(policy.HPC.Manager) != "flux" {
		return nil, nil
	}

	// Get the flux view container (these are choices)
	// ghcr.io/converged-computing/flux-view-rocky:arm-9
	// ghcr.io/converged-computing/flux-view-rocky:arn-8
	// ghcr.io/converged-computing/flux-view-rocky:tag-9
	// ghcr.io/converged-computing/flux-view-rocky:tag-8
	// ghcr.io/converged-computing/flux-view-ubuntu:tag-noble
	// ghcr.io/converged-computing/flux-view-ubuntu:tag-jammy
	// ghcr.io/converged-computing/flux-view-ubuntu:tag-focal
	// ghcr.io/converged-computing/flux-view-ubuntu:arm-jammy
	// ghcr.io/converged-computing/flux-view-ubuntu:arm-focal
	// We use an ubuntu (more recent) default since it is common
	fluxViewImage, ok := policy.HPC.Settings[fluxViewSetting]
	if !ok {
		fluxViewImage = defaultFluxView
	}

	// The JobSet needs to have a headless service for Flux to use
	ensureJobSetNetwork(js, trainJob)

	// We need a custom entrypoint to prepare the view and configure flux
	cm, err := generateConfigMap(js, info, trainJob)
	if err != nil {
		return nil, fmt.Errorf("issue generating config map for Flux HPC entrypoint: %x", err)
	}

	// Shared volumes for Flux install
	// These are "apply configurations" that are needed instead of the volumes
	sharedVolumes := getViewVolumes(*cm)
	initVolumeMounts := []*corev1ac.VolumeMountApplyConfiguration{
		corev1ac.VolumeMount().
			WithName("flux-install").
			WithMountPath("/mnt/flux"),
		corev1ac.VolumeMount().
			WithName("flux-install").
			WithMountPath("/etc/flux-config").
			WithReadOnly(true),
	}
	spackMount := corev1ac.VolumeMount().
		WithName("spack-install").
		WithMountPath("/opt/software")

	// The spack mount (empty dir to copy into) is only needed for the app container
	volumeMounts := append(initVolumeMounts, spackMount)

	// The init container that installs Flux to a shared empty directory
	fluxInstallerContainer := *corev1ac.Container().
		WithName("flux-installer").
		WithImage(fluxViewImage).
		WithCommand("/bin/bash").
		WithArgs("/etc/flux-config/init.sh").
		WithVolumeMounts(initVolumeMounts...)

	// TODO we need an entrypoint for the flux installer container AND for the application to run flux
	// We also need to account for the spack install to /opt/software...
	for i, rJob := range js.ReplicatedJobs {

		// TODO: double check if there is a constant / this is correct
		if *rJob.Name == "trainer" {
			podSpec := js.ReplicatedJobs[i].Template.Spec.Template.Spec
			podSpec.Volumes = append(podSpec.Volumes, sharedVolumes...)
			podSpec.InitContainers = append(podSpec.InitContainers, fluxInstallerContainer)

			// Modify the main application container ("node").
			for j, container := range podSpec.Containers {
				if *container.Name == constants.Node {

					// The main application container needs the install, plus config maps,
					// Plus an empty directory for software
					for _, volumeMount := range volumeMounts {
						container.VolumeMounts = append(container.VolumeMounts, *volumeMount)
					}

					// The container command needs to be our entrypoint to wrap original command with flux
					container.Command = []string{"/bin/bash", "/etc/flux-config/entrypoint.sh"}
					podSpec.Containers[j] = container
				}
			}
		}
	}

	// The Build method returns any new objects that need to be created.
	return []any{cm}, nil
}

func (f *Flux) ReconcilerBuilders() []runtime.ReconcilerBuilder {
	return []runtime.ReconcilerBuilder{
		func(b *builder.Builder, cl client.Client, cache cache.Cache) *builder.Builder {
			return b.Watches(
				&corev1.ConfigMap{},
				handler.EnqueueRequestForOwner(
					f.client.Scheme(), f.client.RESTMapper(), &trainerapi.TrainJob{}, handler.OnlyControllerOwner(),
				),
			)
		},
	}
}

// getJobSetSize ensures we know the number of pods for the cluster
// This assumes a static size - we can allow scaling up/down if desired
func getJobSetSize(js *v1alpha2.JobSetSpecApplyConfiguration) int32 {
	var size int32 = 1
	for _, rJob := range js.ReplicatedJobs {
		// We scope the cluster to only include the trainer job. For the future,
		// there is no reason (given a shared headless service) we can't
		// extend the cluster beyond that. This is a good start for now.
		if *rJob.Name != "trainer" {
			continue
		}

		// Use completions, and default to 1
		if rJob.Template.Spec.Completions != nil {
			size = *rJob.Template.Spec.Completions
		}
		break
	}
	return size
}

// getViewVolumes returns the volume apply configurations for the flux view setup
func getViewVolumes(cm corev1.ConfigMap) []corev1ac.VolumeApplyConfiguration {
	spackInstallAC := corev1ac.Volume().
		WithName("spack-install").
		WithEmptyDir(corev1ac.EmptyDirVolumeSource())
	fluxVolumeAC := corev1ac.Volume().
		WithEmptyDir(corev1ac.EmptyDirVolumeSource()).
		WithName("flux-install")
	cmAC := corev1ac.Volume().
		WithName(cm.Name).
		WithConfigMap(
			corev1ac.ConfigMapVolumeSource().
				WithName(cm.Name).
				WithDefaultMode(0755),
		)
	return []corev1ac.VolumeApplyConfiguration{*spackInstallAC, *fluxVolumeAC, *cmAC}
}

// getJobSetCommand get the original command
func getJobSetCommand(js *v1alpha2.JobSetSpecApplyConfiguration, info *runtime.Info) string {
	for _, rJob := range js.ReplicatedJobs {
		if *rJob.Name != "trainer" {
			continue
		}

		// omg why is this so nested
		if rJob.Template == nil || rJob.Template.Spec == nil || rJob.Template.Spec.Template == nil || rJob.Template.Spec.Template.Spec == nil {
			continue
		}
		podSpec := rJob.Template.Spec.Template.Spec

		// Now get the container command. No command == interactive
		for _, container := range podSpec.Containers {
			if *container.Name == constants.Node {
				if len(container.Command) == 0 && len(container.Args) == 0 {
					return ""
				}
				return strings.Join(append(container.Command, container.Args...), " ")
			}
		}
		break
	}
	return ""
}

// ensureJobSetNetowrk uses the builder pattern to ensure the network is set
func ensureJobSetNetwork(js *v1alpha2.JobSetSpecApplyConfiguration, trainJob *trainerapi.TrainJob) {
	// 1. Get a handle to the existing Network builder, or create a new one if it's nil.
	networkConfig := js.Network
	if networkConfig == nil {
		networkConfig = v1alpha2.Network()
	}

	// Ensure EnableDNSHostnames is explicitly set to true.
	networkConfig.WithEnableDNSHostnames(true)

	// If a subdomain isn't already set, default it to the job name.
	if networkConfig.Subdomain == nil || *networkConfig.Subdomain == "" {
		networkConfig.WithSubdomain(trainJob.Name)
	}

	// 3. Set the (potentially new or modified) network config back onto the main JobSet spec builder.
	// This is the crucial step that applies the changes.
	js.WithNetwork(networkConfig)
}

// prepareFluxView prepares the volume config map
func generateConfigMap(js *v1alpha2.JobSetSpecApplyConfiguration, info *runtime.Info, trainJob *trainerapi.TrainJob) (*corev1.ConfigMap, error) {

	// The entrypoint script needs to configure Flux and copy to the shared volume.
	initScript := generateInitEntrypoint(js, info, trainJob)
	entrypointScript := generateFluxEntrypoint(js, info, trainJob)

	// This needs to be unique / paired with the trainJob
	configMapName := fmt.Sprintf("%s-flux-entrypoint", trainJob.Name)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            configMapName,
			Namespace:       trainJob.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(trainJob, trainerapi.SchemeGroupVersion.WithKind(trainerapi.TrainJobKind))},
		},
		Data: map[string]string{
			"entrypoint.sh": entrypointScript,
			"init.sh":       initScript,
		},
	}
	return cm, nil
}

// getSetting from the info
func getSetting(info *runtime.Info, name, defaultValue string) string {
	value, ok := info.RuntimePolicy.MLPolicySource.HPC.Settings[name]
	if !ok {
		value = defaultValue
	}
	return value
}

// generateBrokerConfig writes the entrypoint file, which prepares the install and configures Flux
func generateBrokerConfig(js *v1alpha2.JobSetSpecApplyConfiguration, info *runtime.Info, trainJob *trainerapi.TrainJob, hosts string) string {

	// Get the network device for Flux to use (or fall back to default)
	networkDevice := getSetting(info, fluxNetworkDevice, defaultNetworkDevice)
	queuePolicy := getSetting(info, fluxQueuePolicy, defaultQueuePolicy)
	fqdn := fmt.Sprintf("%s.%s.svc.cluster.local", *js.Network.Subdomain, trainJob.Namespace)

	// TODO: likely we can derive network device from init container
	// These shouldn't be formatted in block
	defaultBind := "tcp://" + networkDevice + ":%p"
	defaultConnect := "tcp://%h" + fmt.Sprintf(".%s:", fqdn) + "%p"

	// The Flux broker configuration for the Flux Framework HPC cluster
	template := `[access]
allow-guest-user = true
allow-root-owner = true

# Point to resource definition generated with flux-R(1).
[resource]
path = "/mnt/flux/config/etc/flux/system/R"

[bootstrap]
# curve_cert = "/mnt/flux/config/curve/curve.cert"
default_port = 8050
default_bind = "%s"
default_connect = "%s"
hosts = [
{ host="%s"},
]

[archive]
dbpath = "/mnt/flux/config/var/lib/flux/job-archive.sqlite"
period = "1m"
busytimeout = "50s"

[sched-fluxion-qmanager]
queue-policy = "%s"
`
	return fmt.Sprintf(
		template,
		defaultBind,
		defaultConnect,
		hosts,
		queuePolicy,
	)
}

// generateFluxEntrypoint generates the flux entrypoint to prepare the view and run the job
func generateFluxEntrypoint(js *v1alpha2.JobSetSpecApplyConfiguration, info *runtime.Info, trainJob *trainerapi.TrainJob) string {
	mainHost := fmt.Sprintf("%s-0", trainJob.Name)
	command := getJobSetCommand(js, info)

	// TODO we can set strict mode as an option
	script := `#!/bin/sh

fluxuser=$(whoami)
fluxuid=$(id -u $fluxuser)

# Ensure spack view is on the path, wherever it is mounted
viewbase="/mnt/flux"
viewroot=${viewbase}/view
configroot=${viewbase}/config
software="${viewbase}/software"
viewbin="${viewroot}/bin"
fluxpath=${viewbin}/flux

# Important to add AFTER in case software in container duplicated (e.g., Python)
export PATH=$PATH:${viewbin}

# Copy mount software to /opt/software
cp -R ${viewbase}/software/* /opt/software/

# Flux should use the Python with its install
foundroot=$(find $viewroot -maxdepth 2 -type d -path $viewroot/lib/python3\*) > /dev/null 2>&1
pythonversion=$(basename ${foundroot})
pythonversion=${viewroot}/bin/${pythonversion}
echo "Python version: $pythonversion" > /dev/null 2>&1
echo "Python root: $foundroot" > /dev/null 2>&1

# If we found the right python, ensure it's linked (old link does not work)
if [[ -f "${pythonversion}" ]]; then
   rm -rf $viewroot/bin/python3
   rm -rf $viewroot/bin/python
   ln -s ${pythonversion} $viewroot/lib/python  || true
   ln -s ${pythonversion} $viewroot/lib/python3 || true
fi

# Ensure we have flux's python on the path
export PYTHONPATH=${PYTHONPATH:-""}:${foundroot}/site-packages
export FLUX_RC_EXTRA=$viewroot/etc/flux/rc1.d

# Write a script to load fluxion
cat <<EOT >> /tmp/load-fluxion.sh
flux module remove sched-simple
flux module load sched-fluxion-resource
flux module load sched-fluxion-qmanager
EOT
mv /tmp/load-fluxion.sh ${viewbase}/load-fluxion.sh

# Write an easy file we can source for the environment
cat <<EOT >> /tmp/flux-view.sh
#!/bin/bash
export PATH=$PATH
export PYTHONPATH=$PYTHONPATH
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-""}:$viewroot/lib
export fluxsocket=local://${configroot}/run/flux/local
EOT
mv /tmp/flux-view.sh ${viewbase}/flux-view.sh

# Variables we can use again
cfg="${configroot}/etc/flux/config"
command="%s"
    
# Ensure the flux user owns the curve.cert
# We need to move the curve.cert because config map volume is read only
# curvesrc=/etc/flux-config/curve.cert
# curvepath=$configroot/curve/curve.cert
# Prepare curve certificate!
# mkdir -p $configroot/curve
# cp $curvesrc $curvepath

# Remove group and other read
# chmod o-r ${curvepath}
# chmod g-r ${curvepath}
# chown -R ${fluxuid} ${curvepath}

# Generate host resources
hosts=$(cat ${configroot}/etc/flux/system/hostlist)
flux R encode --hosts=${hosts} --local > /tmp/R
mv /tmp/R ${configroot}/etc/flux/system/R

# Put the state directory in /var/lib on shared view
export STATE_DIR=${configroot}/var/lib/flux
export FLUX_OUTPUT_DIR=/tmp/fluxout
mkdir -p ${STATE_DIR} ${FLUX_OUTPUT_DIR}

# Main host <name>-0 and the fully qualified domain name
mainHost="%s"
workdir=$(pwd)

# Make cron.d directory
mkdir -p ${configroot}/etc/flux/system/cron.d
brokerOptions="-Scron.directory=${configroot}/etc/flux/system/cron.d \
  -Stbon.fanout=256 \
  -Srundir=${configroot}/run/flux  \
  -Sstatedir=${STATE_DIR} -Slocal-uri=local://$configroot/run/flux/local \
  -Slog-stderr-level=0  \
  -Slog-stderr-mode=local"

# Run an interactive cluster, giving no command to flux start
function run_interactive_cluster() {
    echo "🌀 flux broker --config-path ${cfg} ${brokerOptions}"
    flux broker --config-path ${cfg} ${brokerOptions}
}

# Start flux with the original entrypoint
if [ $(hostname) == "${mainHost}" ]; then
    
  echo "Command provided is: ${command}" > /dev/null 2>&1
  if [ "${command}" == "" ]; then
    run_interactive_cluster
  else
    
    # If tasks are == 0, then only define nodes
    node_spec="-n2"
    node_spec="${node_spec}"
    flags="${node_spec}  "
    echo "Flags for flux are ${flags}" > /dev/null 2>&1
    flux start  -o --config ${cfg} ${brokerOptions} flux submit ${flags} --quiet --watch ${command}
  fi

# Block run by workers
else

  # We basically sleep/wait until the lead broker is ready
  echo "🌀 flux start  -o --config ${configroot}/etc/flux/config ${brokerOptions}"

    # We can keep trying forever, don't care if worker is successful or not
    # Unless retry count is set, in which case we stop after retries
    while true
    do
        flux start -o --config ${configroot}/etc/flux/config ${brokerOptions}
        retval=$?
        if [[ "${retval}" -eq 0 ]] || [[ "false" == "true" ]]; then
             echo "The follower worker exited cleanly. Goodbye!"
             break
        fi
        echo "Return value for follower worker is ${retval}"
        echo "😪 Sleeping 15s to try again..."
        sleep 15
    done
fi

# Marker of completion, if needed
touch $viewbase/flux-operator-complete.txt
`

	return fmt.Sprintf(
		script,
		command,
		mainHost,
	)
}

// generateInitEntrypoint generates the flux entrypoint to prepare flux
func generateInitEntrypoint(js *v1alpha2.JobSetSpecApplyConfiguration, info *runtime.Info, trainJob *trainerapi.TrainJob) string {

	// fluxRoot for the view is in /opt/view/lib
	// This must be consistent between the flux-view containers
	// github.com:converged-computing/flux-views.git
	fluxRoot := "/opt/view"
	mainHost := fmt.Sprintf("%s-0", trainJob.Name)

	// Generate hostlists. The hostname (prefix) is the trainJob Name
	// We need the initial jobset size, and container command
	size := getJobSetSize(js)
	hosts := generateHostlist(trainJob.Name, size)
	brokerConfig := generateBrokerConfig(js, info, trainJob, hosts)
	setup := `#!/bin/sh
fluxroot=%s
mainHost=%s

# We need to "install" config assets separately. We may not have write to /opt/view.
installRoot=/mnt/flux/config
echo "Hello I am hostname $(hostname) running setup."

# Always use verbose, no reason to not here
echo "Flux install root: ${fluxroot}"
export fluxroot

# Add flux to the path (if using view)
export PATH=/opt/view/bin:$PATH

# If the view doesn't exist, ensure basic paths do
mkdir -p $fluxroot/bin

# Cron directory
mkdir -p $installRoot/etc/flux/system/cron.d
mkdir -p $installRoot/var/lib/flux

# These actions need to happen on all hosts
mkdir -p $installRoot/etc/flux/system
hosts="%s"

# Echo hosts here in case the main container needs to generate
echo "${hosts}" > ${installRoot}/etc/flux/system/hostlist

# Write the broker configuration
mkdir -p ${installRoot}/etc/flux/config
cat <<EOT >> ${installRoot}/etc/flux/config/broker.toml
%s
EOT

echo
echo "🐸 Broker Configuration"
cat ${installRoot}/etc/flux/config/broker.toml

# The rundir needs to be created first, and owned by user flux
# Along with the state directory and curve certificate
mkdir -p ${installRoot}/run/flux ${installRoot}/etc/curve

# TODO: we can add this in if needed, but we'd need to build with added libraries
# View the curve certificate
# echo "🌟️ Curve Certificate"
# cat /etc/flux-config/curve.cert

viewroot=/mnt/flux
mkdir -p $viewroot/view

# Now prepare to copy finished spack view over
echo "Moving content from /opt/view to be in shared volume at $viewroot"
# Note that /opt/view is a symlink to here
view=$(ls /opt/views/._view/)
view="/opt/views/._view/${view}"

# We have to move both of these paths - spack makes link to /opt/software
# /opt/software will need to be restored in application container
cp -R ${view}/* $viewroot/view
cp -R /opt/software $viewroot/

# This is a marker to indicate the copy is done
touch $viewroot/flux-operator-done.txt
echo "Application is done."
`

	return fmt.Sprintf(
		setup,
		fluxRoot,
		mainHost,
		hosts,
		brokerConfig,
	)
}
