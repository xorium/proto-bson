package codec

import (
	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

type Config struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
	Debug    bool   `env:"DEBUG" envDefault:"false"`
}

var CFG = Config{}

func ParseCodecConfigFromEnv() error {
	_ = godotenv.Load()
	if err := env.Parse(&CFG); err != nil {
		return err
	}
	InitLogger()
	return nil
}

func InitLogger() {
	Logger, _ = zap.NewProduction(zap.WithCaller(true))
	defer func() { _ = Logger.Sync() }()
}

func init() {
	_ = ParseCodecConfigFromEnv()
}
