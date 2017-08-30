package scheduler

import (

  	"fmt"
	"os"
	"io/ioutil"
	//"strings"
	//"path/filepath"
	//"time"
    "log"
	//"k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/apimachinery/pkg/runtime"
    sv1 "k8s.io/client-go/pkg/apis/storage/v1beta1"
	//clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	//volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	//bv1 "k8s.io/client-go/pkg/apis/batch/v1"

)

type kdriver struct {
}

 const (
	defaultServer string = "http://127.0.0.1:8001"
	serverEnvVar  string = "SERVER"
	tokenEnvVar   string = "TOKEN"
	caFileEnvVar  string = "CA_FILE"
)

var logger *log.Logger

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
	//gopath=os.Getenv(GOPATH)

	token = os.Getenv("tokenEnvVar")
   // fmt.Println(token)
	caFile := os.Getenv("caFileEnvVar")
	if len(caFile) > 0 {
		caData, err = ioutil.ReadFile(caFile)
	}

	return token, caData, err
}

func convertYamltoStruct(yaml string) runtime.Object {
	
		d := scheme.Codecs.UniversalDeserializer()
		obj, _, err := d.Decode([]byte(yaml), nil, nil)
		if err != nil {
			log.Fatalf("could not decode yaml: %s\n%s", yaml, err)
		}
	
		return obj
}

func createPVC(task Task) *v1.PersistentVolumeClaim {
	
pvc := `
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
 name: ` + task.Vol.Name + `
 annotations:
  volume.beta.kubernetes.io/storage-class: portworx-sc
spec:
 accessModes:
 - ReadWriteOnce
resources:
 requests:
  storage: 2Gi
`
		obj := convertYamltoStruct(pvc)
	
		pvClaim := obj.(*v1.PersistentVolumeClaim)
		//pvC, err := clientset.Core().PersistentVolumeClaims("kube-system").Create(pvClaim)
	
		return pvClaim
}


func createSC(task Task) *sv1.StorageClass {
sc := `
kind: StorageClass	
apiVersion: storage.k8s.io/v1beta1
metadata:
 name: ` + task.Vol.Name + `
provisioner: kubernetes.io/portworx-volume
parameters:
 repl: "1"
	`
	obj := convertYamltoStruct(sc)
	storageClass := obj.(*sv1.StorageClass)
	//	storageClass, err := clientset.Storage().StorageClasses().Create(sClass)

	return storageClass
}




func (k *kdriver) Init() error {
	log.Printf("Using the Kuberntes scheduler driver.\n")
	log.Printf("The following hosts are in the cluster: %v.\n", nodes)
	return nil
}

func (k *kdriver) GetNodes() ([]string, error) {
	return nodes, nil
}

func (k *kdriver) Create(task Task) (*Context, error) {

	clientset,err := kconnect(task.IP)
	if err != nil {
		return nil,err
	}

	context := Context{}
    
    pod := v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "POD",
			APIVersion: "extensions/v1",
		},

		ObjectMeta: metav1.ObjectMeta{
			Name:   task.Name,
			Labels: map[string]string{"app": task.Name},
		},

		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				v1.Volume{
					Name: task.Vol.Name,
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-torpedo",
						},
					},
				},
			},

			Containers: []v1.Container{
				v1.Container{
					Name:            task.Name,
					Image:           task.Img,
					ImagePullPolicy: v1.PullIfNotPresent,
					Command:         task.Cmd,
					VolumeMounts: []v1.VolumeMount{
						v1.VolumeMount{
							MountPath: task.Vol.Path,
							Name:      task.Vol.Name,
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
		Status: v1.PodStatus{},
	}
	 
	context.Task = task
	context.ID = task.Name
	context.PodSpec= &pod

	// create pvc
	pvc := createPVC(task)
	_, err = clientset.Core().PersistentVolumeClaims("kube-system").Create(pvc)

	if err != nil {
		return nil, err
	}

	// create sc
	sc := createSC(task)

	_, err = clientset.StorageV1beta1().StorageClasses().Create(sc)

	if err != nil {
		return nil, err
	}



	return &context,nil

}


func (k *kdriver) Run(ctx *Context) error {
   return nil
}

func (k *kdriver) Start(ctx *Context) error {
    
	clientset,err := kconnect(ctx.Task.IP)
	if err != nil {
		return err
	}

	_, err = clientset.Core().Pods("kube-system").Create(ctx.PodSpec)
	
	if err != nil {
			return err
	}
	return nil
}


func (k *kdriver) WaitDone(ctx *Context) error {


	return nil
	
}


func (k *kdriver) InspectVolume(ip, name string) (*Volume, error) {
 
	clientset,err := kconnect(ip)
	if err != nil {
		return nil,err
	}
  
	   vol,err:=clientset.StorageV1().StorageClasses().Get(name,metav1.GetOptions{})

	v := Volume{
		// Size:   sz,
		Driver: vol.Provisioner,
	}
	return &v, nil
}	

func (k *kdriver) DeleteVolume(ip, name string) error {
	
	clientset,err := kconnect(ip)
	if err != nil {
		return err
	}

	
	err =clientset.PersistentVolumeClaims("default").Delete("test-disk",&metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	return nil

}	


func (k *kdriver) DestroyByName(ip, name string) error {
   
	clientset,err := kconnect(ip)
	if err != nil {
		return err
	}
    
    err=clientset.Pods("default").Delete("name",&metav1.DeleteOptions{})
	if(err!=nil){
			fmt.Println(err)
	}
    

	log.Printf("Deleted task: %v\n", name)

	return nil
}


func (k *kdriver) Destroy(ctx *Context) error {
	clientset,err := kconnect(ctx.Task.IP)
	if err != nil {
		return err
	}
    
    err=clientset.Pods("default").Delete("ctx.ID",&metav1.DeleteOptions{})
	if(err!=nil){
			fmt.Println(err)
	}
    

	log.Printf("Deleted task: %v\n", ctx.Task.Name)
	return nil
}

func init() {
	register("kubernetes", &driver{})
}

