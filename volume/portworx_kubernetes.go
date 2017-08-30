

package volume

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
	"io/ioutil"
	

	"github.com/libopenstorage/openstorage/api"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/volume"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/apimachinery/pkg/runtime"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/pkg/api/v1"
	dv1 "k8s.io/client-go/pkg/apis/extensions/v1beta1"
	av1 "k8s.io/client-go/pkg/apis/rbac/v1beta1"

)

type portworxK8s struct {
	
	clusterManager cluster.Cluster
	volDriver      volume.VolumeDriver
}

var logger *log.Logger

func convertYamltoStruct(yaml string) runtime.Object {
	
		d := scheme.Codecs.UniversalDeserializer()
		obj, _, err := d.Decode([]byte(yaml), nil, nil)
		if err != nil {
			log.Fatalf("could not decode yaml: %s\n%s", yaml, err)
		}
	
		return obj
}

func kconnect(ip string)(*kubernetes.Clientset,error){
	
	 token, caData, err := parseConnectionParams()
	 if err != nil {
		 logger.Fatalf("failed to parse configuration parameters: %s", err)
	 }
 
 
	 config := &rest.Config{
		 Host:            ip,
		 BearerToken:     token,
		 TLSClientConfig: rest.TLSClientConfig{CAData: caData},
	 }
 
 
	 clientset, err := kubernetes.NewForConfig(config)
 
	 
	 return clientset,err
 }

func parseConnectionParams() (token string, caData []byte, err error) {
	

	token = os.Getenv("tokenEnvVar")
   // fmt.Println(token)
	caFile := os.Getenv("caFileEnvVar")
	if len(caFile) > 0 {
		caData, err = ioutil.ReadFile(caFile)
	}

	return token, caData, err
}



func (d *portworxK8s) String() string {
	return "pxd_k8s"
}

func (d *portworxK8s) Init() error {
	log.Printf("Using the Portworx volume portworx.\n")

	n := "127.0.0.1"
	if len(nodes) > 0 {
		n = nodes[1]
	}

	clnt, err := clusterclient.NewClusterClient("http://"+n+":9001", "v1")
	if err != nil {
		return err
	}
	d.clusterManager = clusterclient.ClusterManager(clnt)

	clnt, err = volumeclient.NewDriverClient("http://"+n+":9001", "pxd", "","")
	if err != nil {
		return err
	}
	d.volDriver = volumeclient.VolumeDriver(clnt)

	cluster, err := d.clusterManager.Enumerate()
	if err != nil {
		return err
	}

	log.Printf("The following Portworx nodes are in the cluster:\n")
	for _, n := range cluster.Nodes {
		log.Printf(
			"\tNode ID: %v\tNode IP: %v\tNode Status: %v\n",
			n.Id,
			n.DataIp,
			n.Status,
		)
	}

	return nil
}

func (d *portworxK8s) RemoveVolume(name string) error {
	locator := &api.VolumeLocator{}

	volumes, err := d.volDriver.Enumerate(locator, nil)
	if err != nil {
		return err
	}

	for _, v := range volumes {
		if v.Locator.Name == name {
			// First unmount this volume at all mount paths...
			for _, path := range v.AttachPath {
				if err = d.volDriver.Unmount(v.Id, path); err != nil {
					err = fmt.Errorf(
						"Error while unmounting %v at %v because of: %v",
						v.Id,
						path,
						err,
					)
					log.Printf("%v", err)
					return err
				}
			}

			if err = d.volDriver.Detach(v.Id,true); err != nil {
				err = fmt.Errorf(
					"Error while detaching %v because of: %v",
					v.Id,
					err,
				)
				log.Printf("%v", err)
				return err
			}

			if err = d.volDriver.Delete(v.Id); err != nil {
				err = fmt.Errorf(
					"Error while deleting %v because of: %v",
					v.Id,
					err,
				)
				log.Printf("%v", err)
				return err
			}

			log.Printf("Succesfully removed Portworx volume %v\n", name)

			return nil
		}
	}

	return nil
}


func (d *portworxK8s) Stop(ip string) error{
 
	clientset,err:=kconnect(ip)
	if(err!=nil){
		log.Println(err)
		return err
	}

	err=clientset.ServiceAccounts("kube-system").Delete("px-account",&metav1.DeleteOptions{})
	if(err!=nil){
		 log.Println(err)
		 return err
	}
  
	err=clientset.Rbac().ClusterRoles().Delete("node-get-put-list-role",&metav1.DeleteOptions{})
	if(err!=nil){
	  log.Println(err)
	  return err
	}
  
	err=clientset.Rbac().ClusterRoleBindings().Delete("node-role-binding",&metav1.DeleteOptions{})
	if(err!=nil){
	  log.Println(err)
	  return err
	}
  
	err=clientset.Services("kube-system").Delete("portworx-service",&metav1.DeleteOptions{})
	if(err!=nil){
	  log.Println(err)
	  return err
	}
   
	fal:=false
	
    daemonDelete :=&metav1.DeleteOptions{
	  OrphanDependents:   &fal,
	}
	err=clientset.DaemonSets("kube-system").Delete("portworx",daemonDelete)
	if(err!=nil){
		log.Println(err)
		return err
	}

	log.Println("Portworx Driver Down")

	return nil
}



func (d *portworxK8s) Start(ip string) error{
	clientset,err:=kconnect(ip)
	if(err!=nil){
		log.Println(err)
		return err
	}	

	serviceAccount := `
	apiVersion: v1
	kind: ServiceAccount
	metadata:
	  name: px-account
	  namespace: kube-system
	`
  
	  obj := convertYamltoStruct(serviceAccount)
	  src := obj.(*v1.ServiceAccount)
  
	  src, err = clientset.Core().ServiceAccounts("kube-system").Create(src)
  
	  if err != nil {
		  panic(err.Error())
	  }
  
	  clusterRole := `
	kind: ClusterRole
	apiVersion: rbac.authorization.k8s.io/v1beta1
	metadata:
	  name: node-get-put-list-role
	rules:
	- apiGroups: [""]
	  resources: ["nodes"]
	  verbs: ["get", "update", "list"]
	- apiGroups: [""]
	  resources: ["pods"]
	  verbs: ["get", "list"]
  `
	  obj = convertYamltoStruct(clusterRole)
	  cr := obj.(*av1.ClusterRole)
  
	  cr, err = clientset.Rbac().ClusterRoles().Create(cr)
  
	  clusterRoleBinding := `
	kind: ClusterRoleBinding
	apiVersion: rbac.authorization.k8s.io/v1beta1
	metadata:
	  name: node-role-binding
	subjects:
	- apiVersion: v1
	  kind: ServiceAccount
	  name: px-account
	  namespace: kube-system
	roleRef:
	  kind: ClusterRole
	  name: node-get-put-list-role
	  apiGroup: rbac.authorization.k8s.io
  `
  
	  obj = convertYamltoStruct(clusterRoleBinding)
	  crb := obj.(*av1.ClusterRoleBinding)
  
	  crb, err = clientset.Rbac().ClusterRoleBindings().Create(crb)
  
	  service := `
	kind: Service
	apiVersion: v1
	metadata:
	  name: portworx-service
	  namespace: kube-system
	spec:
	  selector:
		name: portworx
	  ports:
		- protocol: TCP
		  port: 9001
		  targetPort: 9001
  `
	  obj = convertYamltoStruct(service)
	  ser := obj.(*v1.Service)
  
	  ser, err = clientset.Core().Services("kube-system").Create(ser)
  
	  deamonSet := `
	apiVersion: extensions/v1beta1
	kind: DaemonSet
	metadata:
	  name: portworx
	  namespace: kube-system
	spec:
	  minReadySeconds: 0
	  updateStrategy:
		type: OnDelete
	  template:
		metadata:
		  labels:
			name: portworx
		spec:
		  affinity:
			nodeAffinity:
			  requiredDuringSchedulingIgnoredDuringExecution:
				nodeSelectorTerms:
				- matchExpressions:
				  - key: px/enabled
					operator: NotIn
					values:
					- "false"
				  
				  - key: node-role.kubernetes.io/master
					operator: DoesNotExist
				  
		  hostNetwork: true
		  hostPID: true
		  containers:
			- name: portworx
			  image: portworx/px-enterprise:1.2.9
			  terminationMessagePath: "/tmp/px-termination-log"
			  imagePullPolicy: Always
			  args:
				 ["-k etcd://10.111.113.123:2379",
				  "-c mycluster",
				  "",
				  "",
				  "-a -f",
				  "",
				  "",
				  "",
				  "",
				  "",
				  "",
				  "-x", "kubernetes"]
			  
			  livenessProbe:
				initialDelaySeconds: 840 # allow image pull in slow networks
				httpGet:
				  host: 127.0.0.1
				  path: /status
				  port: 9001
			  readinessProbe:
				periodSeconds: 10
				httpGet:
				  host: 127.0.0.1
				  path: /status
				  port: 9001
			  securityContext:
				privileged: true
			  volumeMounts:
				- name: dockersock
				  mountPath: /var/run/docker.sock
				- name: libosd
				  mountPath: /var/lib/osd:shared
				- name: dev
				  mountPath: /dev
				- name: etcpwx
				  mountPath: /etc/pwx/
				- name: optpwx
				  mountPath: /export_bin:shared
				- name: cores
				  mountPath: /var/cores
				- name: kubelet
				  mountPath: /var/lib/kubelet:shared
				- name: src
				  mountPath: /usr/src
				- name: dockerplugins
				  mountPath: /run/docker/plugins
		  restartPolicy: Always
		  
		  serviceAccountName: px-account
		  volumes:
			- name: libosd
			  hostPath:
				path: /var/lib/osd
			- name: dev
			  hostPath:
				path: /dev
			- name: etcpwx
			  hostPath:
				path: /etc/pwx
			- name: optpwx
			  hostPath:
				path: /opt/pwx/bin
			- name: cores
			  hostPath:
				path: /var/cores
			- name: kubelet
			  hostPath:
				path: /var/lib/kubelet
			- name: src
			  hostPath:
				path: /usr/src
			- name: dockerplugins
			  hostPath:
				path: /run/docker/plugins
			- name: dockersock
			  hostPath:
				path: /var/run/docker.sock
  `
	  obj = convertYamltoStruct(deamonSet)
	  dms := obj.(*dv1.DaemonSet)
	  dms, err = clientset.DaemonSets("kube-system").Create(dms)	
	
	log.Printf("Portworx Driver is running")
	return nil
}

func (d *portworxK8s) WaitStart(ip string) error {
	// Wait for Portworx to become usable.
	status, _ := d.clusterManager.NodeStatus()
	for i := 0; status != api.Status_STATUS_OK; i++ {
		if i > 60 {
			return fmt.Errorf(
				"Portworx did not start up in time: Status is %v",
				status,
			)
		}

		time.Sleep(1 * time.Second)
		status, _ = d.clusterManager.NodeStatus()
	}

	return nil
}


func init() {
	nodes = strings.Split(os.Getenv("CLUSTER_NODES"), ",")

	register("pxd_k8s", &portworxK8s{})
}

