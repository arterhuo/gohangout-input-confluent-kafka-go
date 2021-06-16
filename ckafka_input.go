package main

import (
	"github.com/childe/gohangout/codec"
	"github.com/golang/glog"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"reflect"
)

type CkafkaInput struct {
	config         map[interface{}]interface{}
	decorateEvents bool

	messages chan *kafka.Message

	decoder codec.Decoder

	consumers []*kafka.Consumer
}

func New(config map[interface{}]interface{}) interface{} {
	var (
		codertype      string = "plain"
		decorateEvents        = false
		topics         map[interface{}]interface{}
		assign         map[string][]int
	)

	consumer_settings := make(map[string]interface{})
	if v, ok := config["consumer_settings"]; !ok {
		glog.Fatal("kafka input must have consumer_settings")
	} else {
		for x, y := range v.(map[interface{}]interface{}) {
			if reflect.TypeOf(y).Kind() == reflect.Map {
				yy := make(map[string]interface{})
				for kk, vv := range y.(map[interface{}]interface{}) {
					yy[kk.(string)] = vv
				}
				consumer_settings[x.(string)] = yy
			} else {
				consumer_settings[x.(string)] = y
			}
		}
	}
	if v, ok := config["topic"]; ok {
		topics = v.(map[interface{}]interface{})
	} else {
		topics = nil
	}
	if v, ok := config["assign"]; ok {
		assign = make(map[string][]int)
		for topicName, partitions := range v.(map[interface{}]interface{}) {
			assign[topicName.(string)] = make([]int, len(partitions.([]interface{})))
			for i, p := range partitions.([]interface{}) {
				assign[topicName.(string)][i] = p.(int)
			}
		}
	} else {
		assign = nil
	}

	if topics == nil && assign == nil {
		glog.Fatal("either topic or assign should be set")
	}
	if topics != nil && assign != nil {
		glog.Fatal("topic and assign can not be both set")
	}

	if codecV, ok := config["codec"]; ok {
		codertype = codecV.(string)
	}

	if decorateEventsV, ok := config["decorate_events"]; ok {
		decorateEvents = decorateEventsV.(bool)
	}

	ckafkaInput := &CkafkaInput{
		config:         config,
		decorateEvents: decorateEvents,
		messages:       make(chan *kafka.Message, 10),
		decoder:        codec.NewDecoder(codertype),
	}

	var configMap = kafka.ConfigMap{}
	err := getConfigMap(consumer_settings, &configMap)
	if err != nil {
		glog.Fatalf("get kafka config error: %s", err)
	}
	// Consumer
	if topics != nil {
		for topic, threadCount := range topics {
			for i := 0; i < threadCount.(int); i++ {
				c, err := kafka.NewConsumer(&configMap)
				if err != nil {
					glog.Fatalf("could not init Consumer: %s", err)
				}
				ckafkaInput.consumers = append(ckafkaInput.consumers, c)
				c.SubscribeTopics([]string{topic.(string)}, nil)
				go func() {
					for {
						msg, err := c.ReadMessage(-1)
						if err == nil {
							ckafkaInput.messages <- msg
						} else {
							// The client will automatically try to recover from all errors.
							glog.Errorf("Consumer error: %v (%v)\n", err, msg)
						}
					}
				}()
			}
		}
	} else {
		c, err := kafka.NewConsumer(&configMap)
		if err != nil {
			glog.Fatalf("could not init SimpleConsumer: %s", err)
		}
		ckafkaInput.consumers = append(ckafkaInput.consumers, c)
		var TopicPartitions kafka.TopicPartitions
		for topic, partitions := range assign {
			for _, partition := range partitions {
				topicPartition := kafka.TopicPartition{Topic: &topic, Partition: int32(partition)}
				TopicPartitions = append(TopicPartitions, topicPartition)
			}
		}
		c.Assign(TopicPartitions)
		go func() {
			for {
				msg, err := c.ReadMessage(-1)
				if err == nil {
					ckafkaInput.messages <- msg
				} else {
					// The client will automatically try to recover from all errors.
					glog.Errorf("Consumer error: %v (%v)\n", err, msg)
				}
			}
		}()
	}

	return ckafkaInput
}

func (p *CkafkaInput) ReadOneEvent() map[string]interface{} {
	message := <-p.messages
	topicPartition := message.TopicPartition
	err := topicPartition.Error
	if err != nil {
		glog.Error(err)
		return nil
	}
	event := p.decoder.Decode(message.Value)
	if p.decorateEvents {
		kafkaMeta := make(map[string]interface{})
		kafkaMeta["topic"] = topicPartition.Topic
		kafkaMeta["partition"] = topicPartition.Partition
		kafkaMeta["offset"] = topicPartition.Offset
		event["@metadata"] = map[string]interface{}{"kafka": kafkaMeta}
	}
	return event
}

func getConfigMap(config map[string]interface{}, configMap *kafka.ConfigMap) error {
	for key, value := range config {
		err := configMap.SetKey(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *CkafkaInput) Shutdown() {
	if len(p.consumers) > 0 {
		for _, c := range p.consumers {
			c.Close()
		}
	}
}
