package config

import (
	"errors"
	"flag"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env             string          `yaml:"env" env-default:"local"`
	GRPC            GRPC            `yaml:"grpc"`
	Database        Database        `yaml:"database"`
	ResourceService ResourceService `yaml:"resource_service"`
	Tracer          Tracer          `yaml:"tracer"`
	Kafka           Kafka           `yaml:"kafka"`
}

type GRPC struct {
	Port int `yaml:"port" env:"GRPC_PORT" env-default:"60017"`
}

type Database struct {
	Host     string `yaml:"host" env:"DB_HOST" env-default:"localhost"`
	Port     string `yaml:"port" env:"DB_PORT" env-default:"5432"`
	User     string `yaml:"user" env:"DB_USER" env-default:"postgres"`
	Password string `yaml:"password" env:"DB_PASSWORD" env-default:"postgres"`
	Name     string `yaml:"name" env:"DB_NAME" env-default:"booking_service"`
	SSLMode  string `yaml:"ssl_mode" env-default:"disable"`
}

type ResourceService struct {
	Address string `yaml:"address" env:"RESOURCE_SERVICE_ADDRESS" env-default:"localhost:60008"`
}

type Tracer struct {
	EndPoint    string  `yaml:"end-point" env:"END_POINT"`
	Insecure    bool    `yaml:"insecure" env:"INSECURE"`
	SampleRatio float64 `yaml:"sample-ratio" env:"SAMPLE_RATION"`
}

type Kafka struct {
	Enabled  bool        `yaml:"enabled" env:"KAFKA_ENABLED" env-default:"false"`
	Brokers  []string    `yaml:"brokers" env:"KAFKA_BROKERS" env-separator:"," env-default:"localhost:9092"`
	ClientID string      `yaml:"client_id" env:"KAFKA_CLIENT_ID" env-default:"oregon-booking-service"`
	Topics   KafkaTopics `yaml:"topics"`
}

type KafkaTopics struct {
	UserBooking string `yaml:"user_booking" env:"KAFKA_TOPIC_USER_BOOKING" env-default:"topic.user.booking"`
	AdminCancel string `yaml:"admin_cancel" env:"KAFKA_TOPIC_ADMIN_CANCEL" env-default:"topic.admin.cancel"`
	UserCancel  string `yaml:"user_cancel" env:"KAFKA_TOPIC_USER_CANCEL" env-default:"topic.user.cancel"`
	RemindStart string `yaml:"remind_start" env:"KAFKA_TOPIC_REMIND_START" env-default:"topic.messages.start"`
	RemindEnd   string `yaml:"remind_end" env:"KAFKA_TOPIC_REMIND_END" env-default:"topic.messages.end"`
}

func MustLoad() *Config {
	configPath := fetchConfigPath()

	if configPath == "" {
		panic("config path is empty")
	}

	if _, err := os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
		panic("config file does not exists: " + configPath)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("config path is empty: " + err.Error())
	}

	return &cfg
}

func fetchConfigPath() string {
	var path string

	flag.StringVar(&path, "config", "", "path to config file")
	flag.Parse()

	if path == "" {
		path = os.Getenv("CONFIG_PATH")
	}

	return path
}
