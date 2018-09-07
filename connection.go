package dbx

import (
	"fmt"

	"time"

	"flag"

	"os"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	MysqlDriver    = "mysql"
	PgxDriver      = "pgx"
	PostgresDriver = "postgres"
	Sqlite3Driver = "sqlite3"
)

var configKey string

// DBConfig holds the configuration parameters of main DB connection
type Config struct {
	Ssl    bool   `mapstructure:"ssl"`
	Driver string `mapstructure:"driver"`

	Host string `yaml:"host" mapstructure:"host"`
	Port int    `mapstructure:"port"`

	DBName   string `mapstructure:"db_name"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`

	DoLog             bool
	LogDir            string
	SlowQueryDuration time.Duration

	masterDbName string
}

// New creates an instance of *DBX for specified driver (mysql, pgx)
func New(cfg *Config) (*DBX, error) {
	if cfg == nil {
		return nil, errors.New("no config provided")
	}

	newDbx := &DBX{
		driver:     cfg.Driver,
		slowLogMin: DefaultSlowLogMin,
	}
	newDbx.SetLogger(LogError, os.Stderr)

	dsn, err := generateDsn(cfg)
	if err != nil {
		return nil, err
	}

	sqlxDB, err := sqlx.Connect(cfg.Driver, dsn)
	if err != nil {
		return nil, err
	}

	if err = sqlxDB.Ping(); err != nil {
		return nil, err
	}

	newDbx.db = sqlxDB
	return newDbx, nil
}

// NewTest creates an instance of *DBX especially for test
// Multiple checks are done to ensure we are not working on the live data
// Configuration can either be passed as an argument, or retrieved from the environment variables.
// If none were set, default values are used.
func NewTest(cfg *Config) (*DBX, error) {
	if !isTest() {
		return nil, errors.New("you can only run NewTest in test environment")
	}

	if cfg == nil {
		return nil, errors.New("no config provided")
	}

	if cfg.DBName == "" {
		return nil, errors.New("db name is empty")
	}

	// If db name starts with prefix 'test_' we use it
	if isTestableDb(cfg.DBName) {
		return New(cfg)
	}

	// Otherwise we connect to it and attempt to create
	// a new test database with a randomly generated name 'test_1243234284383'
	db, err := New(cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// We set the master db name as the original one, and DBName becomes the new test db name
	// This is done so we can later delete the newly created test db by connecting to the master
	cfg.masterDbName = cfg.DBName
	cfg.DBName = fmt.Sprintf("test_%d", time.Now().UnixNano())

	_, err = db.Exec(fmt.Sprintf("Create Database %s;", cfg.DBName))
	if err != nil {
		return nil, err
	}

	return New(cfg)
}

func DropTestDB(cfg *Config) error {
	if !isTest() {
		return errors.New("you can only run NewTest in test environment")
	}

	if cfg == nil {
		return errors.New("no config provided")
	}

	if cfg.DBName == "" {
		return errors.New("db name is empty")
	}

	// If db name starts with prefix 'test_' we use it
	if !isTestableDb(cfg.DBName) {
		return errors.New("db name does not start with test_")
	}

	if cfg.masterDbName == "" {
		return errors.New("not dropping test db because master db name not provided")
	}

	testDBName := cfg.DBName
	cfg.DBName = cfg.masterDbName
	cfg.masterDbName = ""

	db, err := New(cfg)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE %s", testDBName))
	return err
}

func SetEnvPrefix(pref string) {
	configKey = pref
}

func generateDsn(cfg *Config) (string, error) {
	switch cfg.Driver {
	case MysqlDriver:
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName), nil
	case PgxDriver:
		fallthrough
	case PostgresDriver:
		ssl := "require"
		if cfg.Ssl == false {
			ssl = "disable"
		}

		return fmt.Sprintf("dbname=%s host=%s port=%d user=%s password=%s sslmode=%s", cfg.DBName, cfg.Host, cfg.Port, cfg.User, cfg.Password, ssl), nil
	case Sqlite3Driver:
		return cfg.Host, nil
	default:
		return "", fmt.Errorf("driver %s not supported", cfg.Driver)
	}
}

func isTest() bool {
	return flag.Lookup("test.v") != nil
}

func isTestableDb(dbName string) bool {
	return dbName[0:5] == "test_"
}

func LoadConfigFromEnv(configPrefix string) (*Config, error) {
	configKey = configPrefix

	v := viper.New()
	setConfigDefaults(v)
	v.SetEnvPrefix(configKey)

	v.BindEnv("driver")
	v.BindEnv("host")
	v.BindEnv("port")
	v.BindEnv("db_name")
	v.BindEnv("user")
	v.BindEnv("password")
	v.BindEnv("ssl")

	v.AutomaticEnv()
	v.AllSettings()

	cfg := &Config{}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func LoadConfigFromFile(cfgKey string, configType string, configName string, configPaths ...string) (*Config, error) {
	configKey = cfgKey

	v := viper.New()
	setConfigDefaults(v)

	v.SetConfigName(configName)
	v.SetConfigType(configType)

	for _, path := range configPaths {
		v.AddConfigPath(parsePath(path))
	}

	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := v.UnmarshalKey(configKey, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func setConfigDefaults(v *viper.Viper) {
	v.SetDefault("driver", PgxDriver)
	v.SetDefault("host", "localhost")
	v.SetDefault("port", 5432)
	v.SetDefault("db_name", "postgres")
	v.SetDefault("user", "root")
	v.SetDefault("password", "root")
	v.SetDefault("ssl", false)
}
