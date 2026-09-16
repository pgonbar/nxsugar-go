package nxsugar

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jaracil/ei"
)

type serverConfig struct {
	Name         string
	Url          string
	User         string
	Pass         string
	LogLevel     string
	GracefulExit float64
	Testing      bool
	Pulls        int
	PullTimeout  float64
	MaxThreads   int
	StatsPeriod  float64
	Services     map[string]serviceConfig
	Version      string
}

type serviceConfig struct {
	Description string
	Path        string
	Pulls       int
	PullTimeout float64
	MaxThreads  int
	Version     string
}

// ServerFromConfig represents a server loaded from the configuration file to which services can be added.
type ServerFromConfig struct {
	Server
}

var config map[string]interface{}
var configParsed = false
var configServer serverConfig
var configFile string

var productionModeSet = false
var productionMode bool

/*
MissingConfigErr is used for standard logging when custom service config parameter has been not found.
*/
var MissingConfigErr = "missing parameter (%s) on config file"

/*
InvalidConfigErr is used for standard logging when custom service config parameter is invalid.
*/
var InvalidConfigErr = "invalid parameter (%s) on config file: %s"
var _logLevels = []string{"debug", "info", "warn", "error", "fatal", "panic"}

func parseConfig() (error, map[string]interface{}) {
	if !configParsed {
		configServer = serverConfig{
			LogLevel:     "info",
			GracefulExit: 20,
			Testing:      false,
			Pulls:        1,
			PullTimeout:  3600,
			MaxThreads:   4,
			StatsPeriod:  1800,
			Services:     map[string]serviceConfig{},
		}

		// Open file
		cf, err := os.Open(configFile)
		defer cf.Close()
		if err != nil {
			return fmt.Errorf("can't open config file (%s): %s", configFile, err.Error()), ei.M{"type": "config_file"}
		}

		// Parse file
		dec := json.NewDecoder(cf)
		config = map[string]interface{}{}
		err = dec.Decode(&config)
		if err != nil {
			return fmt.Errorf("can't parse config file (%s): %s", configFile, err.Error()), ei.M{"type": "config_file"}
		}

		// Get server config
		if _, ok := config["server"]; !ok {
			return fmt.Errorf(MissingConfigErr, "server"), ei.M{"type": "missing_param"}
		}
		server, err := ei.N(config).M("server").MapStr()
		if err != nil {
			return fmt.Errorf(InvalidConfigErr, "server", "must be map"), ei.M{"type": "invalid_param"}
		}
		if name, ok := server["name"]; ok {
			if configServer.Name, err = ei.N(name).String(); err != nil {
				return fmt.Errorf(InvalidConfigErr, "server.name", "must be string"), ei.M{"type": "invalid_param"}
			}
		}
		if _, ok := server["url"]; !ok {
			return fmt.Errorf(MissingConfigErr, "server.url"), ei.M{"type": "missing_param"}
		}
		if configServer.Url, err = ei.N(server).M("url").String(); err != nil {
			return fmt.Errorf(InvalidConfigErr, "server.user", "must be string"), ei.M{"type": "invalid_param"}
		}
		if _, ok := server["user"]; !ok {
			return fmt.Errorf(MissingConfigErr, "server.user"), ei.M{"type": "missing_param"}
		}
		if configServer.User, err = ei.N(server).M("user").String(); err != nil {
			return fmt.Errorf(InvalidConfigErr, "server.user", "must be string"), ei.M{"type": "invalid_param"}
		}
		if _, ok := server["pass"]; !ok {
			return fmt.Errorf(MissingConfigErr, "server.pass"), ei.M{"type": "missing_param"}
		}
		if configServer.Pass, err = ei.N(server).M("pass").String(); err != nil {
			return fmt.Errorf(InvalidConfigErr, "server.pass", "must be string"), ei.M{"type": "invalid_param"}
		}
		if lli, ok := server["log-level"]; ok {
			if ll, err := ei.N(lli).String(); err == nil {
				if !inStrSlice(ll, _logLevels) {
					return fmt.Errorf(InvalidConfigErr, "server.log-level", fmt.Sprintf("must be one of [%+v]", _logLevels)), ei.M{"type": "invalid_param"}
				}
				configServer.LogLevel = ll
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.log-level", "must be string"), ei.M{"type": "invalid_param"}
			}
		}
		if gei, ok := server["graceful-exit"]; ok {
			if ge, err := ei.N(gei).Float64(); err == nil {
				if ge < 0 {
					return fmt.Errorf(InvalidConfigErr, "server.graceful-exit", "must be positive"), ei.M{"type": "invalid_param"}
				}
				configServer.GracefulExit = ge
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.graceful-exit", "must be float"), ei.M{"type": "invalid_param"}
			}
		}
		if tsi, ok := server["testing"]; ok {
			if ts, err := ei.N(tsi).Bool(); err == nil {
				configServer.Testing = ts
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.testing", "must be bool"), ei.M{"type": "invalid_param"}
			}
		}
		if psi, ok := server["pulls"]; ok {
			if ps, err := ei.N(psi).Int(); err == nil {
				if ps < 1 {
					return fmt.Errorf(InvalidConfigErr, "server.pull", "must be positive"), ei.M{"type": "invalid_param"}
				}
				configServer.Pulls = ps
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.pull", "must be int"), ei.M{"type": "invalid_param"}
			}
		}
		if pti, ok := server["pull-timeout"]; ok {
			if pt, err := ei.N(pti).Float64(); err == nil {
				if pt < 0 {
					return fmt.Errorf(InvalidConfigErr, "server.pull-timeout", "must be positive or 0"), ei.M{"type": "invalid_param"}
				}
				configServer.PullTimeout = pt
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.pull-timeout", "must be float"), ei.M{"type": "invalid_param"}
			}
		}
		if mti, ok := server["max-threads"]; ok {
			if mt, err := ei.N(mti).Int(); err == nil {
				if mt < 1 {
					return fmt.Errorf(InvalidConfigErr, "server.max-threads", "must be positive"), ei.M{"type": "invalid_param"}
				}
				configServer.MaxThreads = mt
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.max-threads", "must be int"), ei.M{"type": "invalid_param"}
			}
		}
		if spi, ok := server["stats-period"]; ok {
			if sp, err := ei.N(spi).Float64(); err == nil {
				if sp < 1 {
					return fmt.Errorf(InvalidConfigErr, "server.stats-period", "must be positive"), ei.M{"type": "invalid_param"}
				}
				configServer.StatsPeriod = sp
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.stats-period", "must be float"), ei.M{"type": "invalid_param"}
			}
		}
		if vrs, ok := server["version"]; ok {
			if vs, err := ei.N(vrs).String(); err == nil {
				configServer.Version = vs
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.version", "must be string"), ei.M{"type": "invalid_param"}
			}
		}

		// Get services config
		if _, ok := config["services"]; !ok {
			return fmt.Errorf(MissingConfigErr, "services"), ei.M{"type": "missing_param"}
		}
		services, err := ei.N(config).M("services").MapStr()
		if err != nil {
			return fmt.Errorf(InvalidConfigErr, "services", "must be map"), ei.M{"type": "invalid_param"}
		}

		for name, optsi := range services {
			sc := serviceConfig{
				Pulls:       configServer.Pulls,
				PullTimeout: configServer.PullTimeout,
				MaxThreads:  configServer.MaxThreads,
			}
			opts, err := ei.N(optsi).MapStr()
			if err != nil {
				return fmt.Errorf(InvalidConfigErr, "services."+name, "must be map"), ei.M{"type": "invalid_param"}
			}
			sc.Description = ei.N(opts).M("description").StringZ()
			if pt, err := ei.N(opts).M("path").String(); err != nil {
				return fmt.Errorf(MissingConfigErr, "services."+name+".path"), ei.M{"type": "missing_param"}
			} else if pt == "" {
				return fmt.Errorf(InvalidConfigErr, "services."+name+".path", "must not be empty"), ei.M{"type": "invalid_param"}
			} else {
				sc.Path = pt
			}
			if _, ok := opts["pulls"]; ok {
				if ps, err := ei.N(opts).M("pulls").Int(); err == nil {
					if ps < 1 {
						return fmt.Errorf(InvalidConfigErr, "services."+name+".pulls", "must be positive"), ei.M{"type": "invalid_param"}
					}
					sc.Pulls = ps
				} else {
					return fmt.Errorf(InvalidConfigErr, "services."+name+".pulls", "must be int"), ei.M{"type": "invalid_param"}
				}
			}
			if _, ok := opts["pull-timeout"]; ok {
				if pt, err := ei.N(opts).M("pull-timeout").Float64(); err == nil {
					if pt < 0 {
						return fmt.Errorf(InvalidConfigErr, "services."+name+".pull-timeout", "must be positive or 0"), ei.M{"type": "invalid_param"}
					}
					sc.PullTimeout = pt
				} else {
					return fmt.Errorf(InvalidConfigErr, "services."+name+".pull-timeout", "must be float"), ei.M{"type": "invalid_param"}
				}
			}
			if _, ok := opts["max-threads"]; ok {
				if mt, err := ei.N(opts).M("max-threads").Int(); err == nil {
					if mt < 1 {
						return fmt.Errorf(InvalidConfigErr, "services."+name+".max-threads", "must be positive"), ei.M{"type": "invalid_param"}
					}
					sc.MaxThreads = mt
				} else {
					return fmt.Errorf(InvalidConfigErr, "services."+name+".max-threads", "must be int"), ei.M{"type": "invalid_param"}
				}
			}
			if _, ok := opts["version"]; ok {
				if vs, err := ei.N(opts).M("version").String(); err == nil {
					sc.Version = vs
				} else {
					return fmt.Errorf(InvalidConfigErr, "services."+name+".version", "must be string"), ei.M{"type": "invalid_param"}
				}
			}
			configServer.Services[name] = sc
		}
		configParsed = true
	}
	return nil, nil
}

/*
SetConfigFile specifies the file where the config will be parsed
*/
func SetConfigFile(cf string) {
	configFile = cf
}

/*
SetProductionMode specifies wheter the service is in production mode or not
*/
func SetProductionMode(t bool) {
	productionModeSet = true
	productionMode = t
	SetJSONOutput(productionMode)
}

/*
NewServerFromConfig returns a new nexus server from the configuration file or any error while parsing it (with the format of InvalidConfigErr and MissingConfigErr).
Services can be added to the server with calls to `AddService()`.
If the config has not been previously parsed `NewServerFromConfig` parses it.
*/
func NewServerFromConfig() (*ServerFromConfig, error) {
	parseFlags()
	if err, errM := parseConfig(); err != nil {
		errM["error"] = err.Error()
		LogWithFields(ErrorLevel, "config", errM, "invalid config")
		return nil, err
	}
	return &ServerFromConfig{
		Server{
			Name:         configServer.Name,
			Url:          configServer.Url,
			User:         configServer.User,
			Pass:         configServer.Pass,
			Pulls:        configServer.Pulls,
			PullTimeout:  time.Duration(configServer.PullTimeout * float64(time.Second)),
			MaxThreads:   configServer.MaxThreads,
			LogLevel:     configServer.LogLevel,
			StatsPeriod:  time.Duration(configServer.StatsPeriod * float64(time.Second)),
			GracefulExit: time.Duration(configServer.GracefulExit * float64(time.Second)),
			Testing:      configServer.Testing,
			Version:      configServer.Version,
			services:     map[string]*Service{},
		},
	}, nil
}

/*
AddService adds a service to the server from the configuration file and returns it or it returns an error if the service is not present in the configuration file (with the format of MissingConfigErr).
*/
func (s *ServerFromConfig) AddService(name string) (*Service, error) {
	if s.services == nil {
		s.services = map[string]*Service{}
	}
	svcfg, ok := configServer.Services[name]
	if !ok {
		err := fmt.Errorf(MissingConfigErr, "services."+name)
		LogWithFields(ErrorLevel, "config", ei.M{"type": "missing_param", "error": err.Error()}, "missing config param")
		return nil, err
	}
	if svcfg.Version == "" {
		svcfg.Version = configServer.Version
	}
	svc := &Service{Name: name, Description: svcfg.Description, Url: s.Url, User: s.User, Pass: s.Pass, Path: svcfg.Path, Pulls: svcfg.Pulls, PullTimeout: time.Duration(svcfg.PullTimeout * float64(time.Second)), MaxThreads: svcfg.MaxThreads, StatsPeriod: s.StatsPeriod, GracefulExit: s.GracefulExit, LogLevel: s.LogLevel, Version: svcfg.Version, Testing: s.Testing, sharedSchemas: s.sharedSchemas}
	s.services[name] = svc
	if s.logPath == "" {
		s.logPath = "server/" + name
	}
	return svc, nil
}

/*
AddServices adds several services to a ServerFromConfig
If another service was previously added with the same name it will be replaced
*/
func (s *ServerFromConfig) AddServices(services map[string][]Method) (addedServices map[string]*Service, err error) {
	addedServices = make(map[string]*Service)

	for name, methods := range services {
		service, err := s.AddService(name)
		if err != nil {
			return nil, err
		}

		for _, method := range methods {
			if method.Opts == nil {
				method.Opts = &MethodOpts{}
			}
			if err = service.AddMethodSchema(method.Name, method.Schema, method.Func, method.Opts); err != nil {
				return nil, err
			}
		}

		addedServices[name] = service
	}

	return addedServices, nil
}

/*
NewServiceFromConfig returns a new nexus service from the configuration file or any error while parsing it (with the format of InvalidConfigErr and MissingConfigErr).
If the config has not been previously parsed `NewServiceFromConfig` parses it.
*/
func NewServiceFromConfig(name string) (*Service, error) {
	parseFlags()
	if err, errM := parseConfig(); err != nil {
		errM["error"] = err.Error()
		LogWithFields(ErrorLevel, "config", errM, "invalid config")
		return nil, err
	}
	svc, ok := configServer.Services[name]
	if !ok {
		err := fmt.Errorf(MissingConfigErr, "services."+name)
		LogWithFields(ErrorLevel, "config", ei.M{"type": "missing_param", "error": err.Error()}, "missing config param")
		return nil, err
	}
	if svc.Version == "" {
		svc.Version = configServer.Version
	}
	return &Service{
		Name:         name,
		Url:          configServer.Url,
		User:         configServer.User,
		Pass:         configServer.Pass,
		Path:         svc.Path,
		Pulls:        svc.Pulls,
		PullTimeout:  time.Duration(float64(time.Second) * svc.PullTimeout),
		MaxThreads:   svc.MaxThreads,
		LogLevel:     configServer.LogLevel,
		StatsPeriod:  time.Duration(float64(time.Second) * configServer.StatsPeriod),
		GracefulExit: time.Duration(float64(time.Second) * configServer.GracefulExit),
		Testing:      configServer.Testing,
		Version:      svc.Version,
	}, nil
}

/*
GetConfig returns a map with the parsed configuration or any error while parsing it (with the format of InvalidConfigErr and MissingConfigErr).
If the config has not been previously parsed `GetConfig` parses it.
*/
func GetConfig() (map[string]interface{}, error) {
	parseFlags()
	if err, errM := parseConfig(); err != nil {
		errM["error"] = err.Error()
		LogWithFields(ErrorLevel, "config", errM, "invalid config")
		return nil, err
	}
	return config, nil
}
