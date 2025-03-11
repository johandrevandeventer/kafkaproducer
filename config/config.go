package config

type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string // For consumer groups
}

func NewKafkaConfig(brokers []string, topic string, groupID string) *KafkaConfig {
	return &KafkaConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}
}
