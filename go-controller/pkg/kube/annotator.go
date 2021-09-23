package kube

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	kapi "k8s.io/api/core/v1"
)

// Annotator represents the exported methods for handling node annotations
// Implementations should enforce thread safety on the declared methods
type Annotator interface {
	Set(key string, value interface{}) error
	SetWithFailureHandler(key string, value interface{}, failFn FailureHandlerFn) error
	Delete(key string)
	Run() error
}

// FailureHandlerFn is a function called when adding an annotation fails
type FailureHandlerFn func(obj interface{}, key string, val interface{})

type action struct {
	key     string
	val     string
	origVal interface{}
	failFn  FailureHandlerFn
}

type nodeAnnotator struct {
	kube     Interface
	nodeName string

	changes map[string]interface{}
	sync.Mutex
}

// NewNodeAnnotator returns a new annotator for Node objects
func NewNodeAnnotator(kube Interface, nodeName string) Annotator {
	return &nodeAnnotator{
		kube:     kube,
		nodeName: nodeName,
		changes:  make(map[string]interface{}),
	}
}

func (na *nodeAnnotator) Set(key string, val interface{}) error {
	return na.SetWithFailureHandler(key, val, nil)
}

func (na *nodeAnnotator) SetWithFailureHandler(key string, val interface{}, failFn FailureHandlerFn) error {
	na.Lock()
	defer na.Unlock()

	if val == nil {
		na.changes[key] = nil
		return nil
	}

	// Annotations must be either a valid string value or nil; coerce
	// any non-empty values to string
	if reflect.TypeOf(val).Kind() == reflect.String {
		na.changes[key] = val.(string)
	} else {
		bytes, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("failed to marshal %q value %v to string: %v", key, val, err)
		}
		na.changes[key] = string(bytes)
	}

	return nil
}

func (na *nodeAnnotator) Delete(key string) {
	na.Lock()
	defer na.Unlock()
	na.changes[key] = nil
}

func (na *nodeAnnotator) Run() error {
	na.Lock()
	defer na.Unlock()
	if len(na.changes) == 0 {
		return nil
	}

	err := na.kube.SetAnnotationsOnNode(na.nodeName, na.changes)

	// TODO(flaviof): need to resolve this conflict still
	// if err != nil {
	// 	// Let failure handlers clean up
	// 	for _, act := range na.changes {
	// 		if act.failFn != nil {
	// 			act.failFn(na.nodeName, act.key, act.origVal)
	// 		}
	// 	}
	// }

	return err
}

// NewPodAnnotator returns a new annotator for Pod objects
func NewPodAnnotator(kube Interface, podName string, namespace string) Annotator {
	return &podAnnotator{
		kube:      kube,
		podName:   podName,
		namespace: namespace,
		changes:   make(map[string]interface{}),
	}
}

type podAnnotator struct {
	kube      Interface
	podName   string
	namespace string

	changes map[string]interface{}
	sync.Mutex
}

func (pa *podAnnotator) Set(key string, val interface{}) error {
	return pa.SetWithFailureHandler(key, val, nil)
}

func (pa *podAnnotator) SetWithFailureHandler(key string, val interface{}, failFn FailureHandlerFn) error {
	pa.Lock()
	defer pa.Unlock()

	if val == nil {
		pa.changes[key] = nil
		return nil
	}

	// Annotations must be either a valid string value or nil; coerce
	// any non-empty values to string
	if reflect.TypeOf(val).Kind() == reflect.String {
		pa.changes[key] = val.(string)
	} else {
		bytes, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("failed to marshal %q value %v to string: %v", key, val, err)
		}
		pa.changes[key] = string(bytes)
	}

	return nil
}

func (pa *podAnnotator) Delete(key string) {
	pa.Lock()
	defer pa.Unlock()
	pa.changes[key] = nil
}

func (pa *podAnnotator) Run() error {
	pa.Lock()
	defer pa.Unlock()

	if len(pa.changes) == 0 {
		return nil
	}

	err := pa.kube.SetAnnotationsOnPod(pa.namespace, pa.podName, pa.changes)

	// TODO(flaviof): need to resolve this conflict still
	// if err != nil {
	// 	// Let failure handlers clean up
	// 	for _, act := range pa.changes {
	// 		if act.failFn != nil {
	// 			act.failFn(pa.pod, act.key, act.origVal)
	// 		}
	// 	}
	// }

	return err
}

// NewNamespaceAnnotator returns a new annotator for Namespace objects
func NewNamespaceAnnotator(kube Interface, namespace *kapi.Namespace) Annotator {
	return &namespaceAnnotator{
		kube:      kube,
		namespace: namespace,
		changes:   make(map[string]*action),
	}
}

type namespaceAnnotator struct {
	kube      Interface
	namespace *kapi.Namespace

	changes map[string]*action
	sync.Mutex
}

func (na *namespaceAnnotator) Set(key string, val interface{}) error {
	return na.SetWithFailureHandler(key, val, nil)
}

func (na *namespaceAnnotator) SetWithFailureHandler(key string, val interface{}, failFn FailureHandlerFn) error {
	act := &action{
		key:     key,
		origVal: val,
		failFn:  failFn,
	}
	if val != nil {
		// Annotations must be either a valid string value or nil; coerce
		// any non-empty values to string
		if reflect.TypeOf(val).Kind() == reflect.String {
			act.val = val.(string)
		} else {
			bytes, err := json.Marshal(val)
			if err != nil {
				return fmt.Errorf("failed to marshal %q value %v to string: %v", key, val, err)
			}
			act.val = string(bytes)
		}
	}
	na.Lock()
	defer na.Unlock()
	na.changes[key] = act
	return nil
}

func (na *namespaceAnnotator) Delete(key string) {
	na.Lock()
	defer na.Unlock()
	na.changes[key] = &action{key: key}
}

func (na *namespaceAnnotator) Run() error {
	annotations := make(map[string]interface{})
	na.Lock()
	defer na.Unlock()
	for k, act := range na.changes {
		// Ignore annotations that already exist with the same value
		if existing, ok := na.namespace.Annotations[k]; existing != act.val || !ok {
			if act.origVal != nil {
				// Annotation should be updated to new value
				annotations[k] = act.val
			} else {
				// Annotation should be deleted
				annotations[k] = ""
			}
		}
	}
	if len(annotations) == 0 {
		return nil
	}

	err := na.kube.SetAnnotationsOnNamespace(na.namespace.Name, annotations)
	if err != nil {
		// Let failure handlers clean up
		for _, act := range na.changes {
			if act.failFn != nil {
				act.failFn(na.namespace, act.key, act.origVal)
			}
		}
	}
	return err
}
