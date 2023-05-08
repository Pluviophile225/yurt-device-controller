package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos"
	edgex_resp "github.com/edgexfoundry/go-mod-core-contracts/v2/dtos/responses"
	"github.com/edgexfoundry/go-mod-messaging/v3/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	devicev1alpha1 "github.com/openyurtio/device-controller/apis/device.openyurt.io/v1alpha1"
	"k8s.io/klog/v2"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"
	"sync"
	"time"
)

var (
	cc *commandCache
)

type CommandCache interface {
	AllCommand(name string) []dtos.CoreCommand
	SetCommand(name string, coreCommands []dtos.CoreCommand)
	PropertyByDevice(name string) map[string]devicev1alpha1.ActualPropertyState
	PropertyByCommand(deviceName string, commandName string) devicev1alpha1.ActualPropertyState
	SetProperty(deviceName string, commandName string, actualProperty devicev1alpha1.ActualPropertyState)
}

// opts *options.YurtDeviceControllerOptions
type commandCache struct {
	deviceCommandMap  map[string][]dtos.CoreCommand
	devicePropertyMap map[string]map[string]devicev1alpha1.ActualPropertyState
	syncPeriod        time.Duration
	mutex             sync.RWMutex
}

func (c *commandCache) Init() {
	c.deviceCommandMap = make(map[string][]dtos.CoreCommand)
	c.devicePropertyMap = make(map[string]map[string]devicev1alpha1.ActualPropertyState)
	c.syncPeriod = 5000
}

func (c *commandCache) AllCommand(name string) []dtos.CoreCommand {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	deviceCommands, ok := c.deviceCommandMap[name]
	if !ok {
		return nil
	}
	ds := make([]dtos.CoreCommand, len(deviceCommands))
	i := 0
	for _, dc := range deviceCommands {
		ds[i] = dc
		i++
	}
	return ds
}
func (c *commandCache) SetCommand(name string, coreCommands []dtos.CoreCommand) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	ds := make([]dtos.CoreCommand, len(coreCommands))
	i := 0
	for _, dc := range coreCommands {
		ds[i] = dc
		i++
	}
	c.deviceCommandMap[name] = ds
}
func (c *commandCache) PropertyByCommand(deviceName string, commandName string) devicev1alpha1.ActualPropertyState {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	propertyMap, ok := c.devicePropertyMap[deviceName]
	if !ok {
		return devicev1alpha1.ActualPropertyState{}
	}
	propertyState, ok := propertyMap[commandName]
	if !ok {
		return devicev1alpha1.ActualPropertyState{}
	}
	return propertyState
}

func (c *commandCache) SetProperty(deviceName string, commandName string, actualProperty devicev1alpha1.ActualPropertyState) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.devicePropertyMap[deviceName][commandName] = actualProperty
}

func (c *commandCache) PropertyByDevice(name string) map[string]devicev1alpha1.ActualPropertyState {
	commandProperty, ok := c.devicePropertyMap[name]
	if !ok {
		return nil
	}
	return commandProperty
}

func (c *commandCache) Run(stop <-chan struct{}) {
	config := types.MessageBusConfig{
		Broker: types.HostInfo{Host: "localhost", Port: 1883, Protocol: "tcp"},
		Type:   "mqtt",
		Optional: map[string]string{
			"ClientId": "CommandCache",
			"Retained": "true",
		},
	}
	client, err := messaging.NewMessageClient(config)
	if err != nil {
		klog.V(3).ErrorS(err, "fail to create messageClient")
	}
	klog.V(1).Info("[CommandCache] Start")
	go func() {
		for {
			<-time.After(c.syncPeriod)
			klog.V(2).Info("[CommandCache] Start a round of synchronization.")
			// get all device commands
			c.getCommands(client)
			c.setProperties(client)
		}
	}()
	<-stop
	klog.V(1).Info("[CommandCache] Stop")

}
func (c *commandCache) NewDeviceSyncerRunnable() ctrlmgr.RunnableFunc {
	return func(ctx context.Context) error {
		c.Run(ctx.Done())
		return nil
	}
}

func Commands() CommandCache {
	return cc
}
func (c *commandCache) getCommands(client messaging.MessageClient) {
	// edgex/commandquery/request/all:
	messages := make(chan types.MessageEnvelope)
	messageErrors := make(chan error)
	topics := []types.TopicChannel{
		{
			Topic:    GetAllDeviceResponse,
			Messages: messages,
		},
	}
	err := client.Subscribe(topics, messageErrors)
	if err != nil {
		klog.V(3).ErrorS(err, "fail to subscribe message")
	}
	msgEnvelope := types.MessageEnvelope{
		CorrelationID: "14a42ea6-c394-41c3-8bcd-a29b9f5e6835",
		ContentType:   "application/json",
	}
	err = client.Publish(msgEnvelope, GetAllDevice)
	if err != nil {
		klog.V(3).ErrorS(err, "fail to publish message")
	}
	commandResponse := edgex_resp.MultiDeviceCoreCommandsResponse{}
	timer := time.NewTimer(5000)
	select {
	case <-timer.C:
		klog.V(3).Infof("get response timeout")

	case err = <-messageErrors:
		klog.V(3).ErrorS(err, "encountered error waiting for response to %s: %v", topics[0].Topic, err)

	case responseMessage := <-messages:
		err := json.Unmarshal(responseMessage.Payload, &commandResponse)
		if err != nil {
			klog.V(3).ErrorS(err, "json unmarshal fail")
		}
	}
	commands := commandResponse.DeviceCoreCommands
	for _, command := range commands {
		c.SetCommand(command.DeviceName, command.CoreCommands)
	}
}

func (c *commandCache) setProperties(client messaging.MessageClient) {
	stop := make(chan bool)
	messages := make(chan types.MessageEnvelope)
	messageErrors := make(chan error)
	topics := []types.TopicChannel{
		{
			Topic:    GetCommandResponse,
			Messages: messages,
		},
	}
	err := client.Subscribe(topics, messageErrors)
	if err != nil {
		klog.V(3).ErrorS(err, "fail to subscribe message")
	}
	msgEnvelope := types.MessageEnvelope{
		CorrelationID: "14a42ea6-c394-41c3-8bcd-a29b9f5e6835",
		ContentType:   "application/json",
	}
	go func() {
		for {
			select {
			case <-stop:
				klog.V(3).Infof("[setProperties] stop subscribe the messageBus")
			case err = <-messageErrors:
				klog.V(3).ErrorS(err, "encountered error waiting for response to %s: %v", topics[0].Topic, err)
			case requestEnvelope := <-messages:
				var eResp edgex_resp.EventResponse
				if err := json.Unmarshal(requestEnvelope.Payload, &eResp); err != nil {
					klog.V(5).ErrorS(err, "failed to decode the response ", "response", requestEnvelope)
					continue
				}
				event := eResp.Event
				deviceName := event.DeviceName
				sourceName := event.SourceName
				actualValue := getPropertyValueFromEvent(sourceName, event)
				actualProperty := devicev1alpha1.ActualPropertyState{}
				actualProperty.ActualValue = actualValue
				c.SetProperty(deviceName, sourceName, actualProperty)
			}
		}
	}()

	for deviceName, commands := range c.deviceCommandMap {
		for _, command := range commands {
			if command.Get {
				topic := GetCommandTopic + "/" + deviceName + "/" + command.Name + "/get"
				err = client.Publish(msgEnvelope, topic)
				if err != nil {
					klog.V(3).ErrorS(err, "fail to publish message")
				}
			}
		}
	}
	stop <- true
}

// The actual property value is resolved from the returned event
func getPropertyValueFromEvent(resName string, event dtos.Event) string {
	actualValue := ""
	for _, r := range event.Readings {
		if resName == r.ResourceName {
			if r.SimpleReading.Value != "" {
				actualValue = r.SimpleReading.Value
			} else if len(r.BinaryReading.BinaryValue) != 0 {
				// TODO: how to demonstrate binary data
				actualValue = fmt.Sprintf("%s:%s", r.BinaryReading.MediaType, "blob value")
			} else if r.ObjectReading.ObjectValue != nil {
				serializedBytes, _ := json.Marshal(r.ObjectReading.ObjectValue)
				actualValue = string(serializedBytes)
			}
			break
		}
	}
	return actualValue
}
