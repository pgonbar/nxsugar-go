package nxsugar

import (
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xeipuuv/gojsonschema"

	"github.com/jaracil/ei"
	nxcli "github.com/nayarsystems/nxgo"
	nexus "github.com/nayarsystems/nxgo/nxcore"
)

/*
Service pulls tasks from a nexus path and resolves them.
A `Service` can be created with a call to `NewService()` or `NewServiceFromConfig()`. It can also be created by adding it to a `Server` or `ServerFromConfig` with a call to `AddService()`.
Its configuration can be changed with calls to `Set...()`.
Multiple methods or a handler can be added to it with calls to `AddMethod()` or `SetHandler()`.
Once it is configured and its methods added, a call to `Serve()` will start the service.
*/
type Service struct {
	Name          string
	Description   string
	Url           string
	User          string
	Pass          string
	Path          string
	Pulls         int
	PullTimeout   time.Duration
	MaxThreads    int
	StatsPeriod   time.Duration
	GracefulExit  time.Duration
	LogLevel      string
	Version       string
	Testing       bool
	ConnState     func(*NexusConn, NexusConnState)
	nc            *nexus.NexusConn
	methods       map[string]*method
	handler       *method
	sharedSchemas map[string]gojsonschema.JSONLoader
	stats         *Stats
	stopServeCh   chan (bool)
	threadsSem    *semaphore
	wg            *sync.WaitGroup
	stopping      bool
	stopLock      *sync.Mutex
	debugEnabled  bool
	sharedConn    bool
	connId        string
	connLock      sync.Mutex
}

type NexusConnState int

func (cs NexusConnState) String() string {
	return connStateStr[cs]
}

const (
	StateInitializing NexusConnState = iota
	StateConnecting
	StateLoggingIn
	StateServing
	StateStopped
)

var connStateStr = map[NexusConnState]string{
	StateInitializing: "initializing",
	StateConnecting:   "connecting",
	StateLoggingIn:    "logging in",
	StateServing:      "serving",
	StateStopped:      "stopped",
}

type method struct {
	disablePullLog          bool
	enableResponseResultLog bool
	enableResponseErrorLog  bool
	logOnTimeExceeded       time.Duration
	inSchema                *methodSchema
	resSchema               *methodSchema
	errSchema               *methodSchema
	pacts                   []*methodPact
	computedSchema          interface{}
	f                       func(t *Task)
	testf                   func(t *Task)
}

type MethodOpts struct {
	TestFunction            func(*Task) (interface{}, *JsonRpcErr)
	DisablePullLog          bool
	EnableResponseResultLog bool
	EnableResponseErrorLog  bool
	LogOnTimeExceeded       time.Duration
}

/*
Stats holds the statistics of a service.
*/
type Stats struct {
	TaskPullsDone       uint64 `json:"task-pulls-done"`
	TaskPullTimeouts    uint64 `json:"task-pull-timeouts"`
	TasksPulled         uint64 `json:"tasks-pulled"`
	TasksPanic          uint64 `json:"tasks-panic"`
	TasksServed         uint64 `json:"tasks-served"`
	TasksMethodNotFound uint64 `json:"tasks-method-not-found"`
	TasksRunning        uint64 `json:"tasks-running"`
}

/*
ServiceOpts is the configuration for creating a service.
*/
type ServiceOpts struct {
	Pulls       int
	PullTimeout time.Duration
	MaxThreads  int
	Testing     bool
}

/*
NewService creates a new nexus service
If passed ServiceOpts is nil the defaults are 1 pull, an hour of pullTimeout and runtime.NumCPU() maxThreads.
Debug output is disabled by deafult
StatsPeriod defaults to 5 minutes
GracefulExitTime defaults to 20 seconds
*/
func NewService(url string, path string, opts *ServiceOpts) *Service {
	parseFlags()
	url, username, password := parseServerUrl(url)
	opts = populateOpts(opts)
	return &Service{Name: "service", Url: url, User: username, Pass: password, Path: path, Pulls: opts.Pulls, PullTimeout: opts.PullTimeout, MaxThreads: opts.MaxThreads, LogLevel: "info", StatsPeriod: time.Minute * 5, GracefulExit: time.Second * 20, Testing: opts.Testing, Version: "0.0.0"}
}

// Set defaults for Opts
func populateOpts(opts *ServiceOpts) *ServiceOpts {
	if opts == nil {
		opts = &ServiceOpts{
			Pulls:       1,
			PullTimeout: time.Hour,
			MaxThreads:  runtime.NumCPU(),
		}
	}
	if opts.Pulls <= 0 {
		opts.Pulls = 1
	}
	if opts.PullTimeout < 0 {
		opts.PullTimeout = 0
	}
	if opts.MaxThreads <= 0 {
		opts.MaxThreads = 1
	}
	if opts.MaxThreads < opts.Pulls {
		opts.MaxThreads = opts.Pulls
	}
	return opts
}

// Get url, user and pass
func parseServerUrl(server string) (string, string, string) {
	var username string
	var password string
	if !strings.Contains(server, "://") {
		server = "tcp://" + server
	}
	parsed, err := url.Parse(server)
	if err == nil && parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
	}
	return server, username, password
}

// GetConn returns the underlying nexus connection of a service
func (s *Service) GetConn() *NexusConn {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	if s.nc == nil {
		return nil
	}
	return &NexusConn{NexusConn: s.nc}
}

func (s *Service) getConnid() string {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	return s.connId
}

func (s *Service) setConn(nc *nexus.NexusConn) {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	s.nc = nc
	if nc != nil {
		s.sharedConn = true
		s.connId = s.nc.Id()
	} else {
		s.sharedConn = false
		s.connId = ""
	}
}

func (s *Service) addMethod(name string, schema *Schema, f func(*Task) (interface{}, *JsonRpcErr), opts *MethodOpts) error {
	if s.methods == nil {
		s.initMethods()
	}
	s.methods[name] = &method{disablePullLog: opts.DisablePullLog, enableResponseResultLog: opts.EnableResponseResultLog, enableResponseErrorLog: opts.EnableResponseErrorLog, logOnTimeExceeded: opts.LogOnTimeExceeded, f: defMethodWrapper(f), testf: nil, inSchema: nil, resSchema: nil, errSchema: nil, pacts: []*methodPact{}}
	if opts.TestFunction != nil {
		s.methods[name].testf = defMethodWrapper(opts.TestFunction)
	}
	if schema != nil {
		err, errM := s.addSchemaToMethod(name, schema)
		if err != nil {
			s.LogWithFields(ErrorLevel, errM, err.Error())
			return err
		}
	}
	return nil
}

func (s *Service) initMethods() {
	s.methods = map[string]*method{}

	// Add @schema method
	s.methods["@schema"] = &method{
		f: func(t *Task) {
			methodSchemas := map[string]interface{}{}
			for name, m := range s.methods {
				methodSchemas[name] = m.computedSchema
			}

			sendRes := ei.M{
				"methods": methodSchemas,
			}

			if ei.N(t.Params).M("include_shared").BoolZ() {
				sharedSchemas := map[string]interface{}{}
				for name, s := range s.sharedSchemas {
					sharedSchemas[name] = s.JsonSource()
				}
				sendRes["shared"] = sharedSchemas
			}

			_, err := t.SendResultCtx(t.Ctx, sendRes)
			if err != nil {
				s.LogWithFields(ErrorLevel, ei.M{"type": "send_result", "where": fmt.Sprintf("%s%s", t.Path, t.Method)}, "Could not send result: %s", err.Error())
				t.SendErrorCtx(t.Ctx, ErrInternal, "could not send result", nil)
			}
		},
		computedSchema: map[string]interface{}{},
	}

	// Add @info method
	s.methods["@info"] = &method{
		f: func(t *Task) {
			_, err := t.SendResultCtx(t.Ctx, ei.M{
				"name":          s.Name,
				"description":   s.Description,
				"version":       s.Version,
				"nxcli-version": Info.NxcliVersion,
				"wan-ip":        Info.WanIp,
				"lan-ips":       Info.LanIps,
				"user":          Info.User,
				"directory":     Info.Dir,
				"uptime":        int(time.Since(Info.Started).Seconds()),
				"testing":       s.IsTesting(),
				"stats":         *s.stats,
			})
			if err != nil {
				s.LogWithFields(ErrorLevel, ei.M{"type": "send_result", "where": fmt.Sprintf("%s%s", t.Path, t.Method)}, "Could not send result: %s", err.Error())
				t.SendErrorCtx(t.Ctx, ErrInternal, "could not send result", nil)
			}
		},
		computedSchema: map[string]interface{}{},
	}

	// Add @ping method
	s.methods["@ping"] = &method{
		f: func(t *Task) {
			_, err := t.SendResultCtx(t.Ctx, "pong")
			if err != nil {
				s.LogWithFields(ErrorLevel, ei.M{"type": "send_result", "where": fmt.Sprintf("%s%s", t.Path, t.Method)}, "Could not send result: %s", err.Error())
				t.SendErrorCtx(t.Ctx, ErrInternal, "could not send result", nil)
			}
		},
		computedSchema: map[string]interface{}{},
	}
}

func defMethodWrapper(f func(*Task) (interface{}, *JsonRpcErr)) func(*Task) {
	return func(t *Task) {
		res, err := f(t)
		if res != nil {
			t.Tags["@local-response-result"] = res
		}
		if err != nil {
			t.Tags["@local-response-error"] = err
		}
		if _, ok := t.Tags["@local-repliedTo"]; ok {
			return
		}
		if err != nil {
			_, serr := t.SendErrorCtx(t.Ctx, err.Cod, err.Mess, err.Dat)
			if serr != nil {
				t.Service.LogWithFields(ErrorLevel, ei.M{"type": "send_error", "where": fmt.Sprintf("%s%s", t.Path, t.Method)}, "Could not send error: %s", serr.Error())
			}
		} else {
			_, serr := t.SendResultCtx(t.Ctx, res)
			if serr != nil {
				t.Service.LogWithFields(ErrorLevel, ei.M{"type": "send_result", "where": fmt.Sprintf("%s%s", t.Path, t.Method)}, "Could not send result: %s", serr.Error())
				t.SendErrorCtx(t.Ctx, ErrInternal, "could not send result", nil)
			}
		}
	}
}

/*
ReplyToWrapper is a wrapper for methods.
If a replyTo map parameter is set with a type parameter (with "pipe" or "service" values) and a path
parameter with the service path or pipeId to respond to, the usual SendError/SendResult pattern will
be skipped and the answer will go to the pipe or service specified after doing an Accept() to the task.
*/
func ReplyToWrapper(f func(*Task) (interface{}, *JsonRpcErr)) func(*Task) (interface{}, *JsonRpcErr) {
	return func(t *Task) (interface{}, *JsonRpcErr) {
		var repTy, repPath string
		var ok bool
		if replyTo, err := ei.N(t.Params).M("replyTo").MapStr(); err != nil {
			return f(t)
		} else {
			if repPath, ok = replyTo["path"].(string); !ok {
				return f(t)
			}
			if repTy, ok = replyTo["type"].(string); !ok || (repTy != "pipe" && repTy != "service") {
				return f(t)
			}
		}
		res, errm := f(t)
		t.Tags["@local-repliedTo"] = true
		_, err := t.Accept()
		if err != nil {
			t.Service.LogWithFields(WarnLevel, ei.M{"type": "reply_to_accept"}, "could not accept task: %s", err.Error())
		} else if repTy == "pipe" {
			if pipe, err := t.GetConn().PipeOpen(repPath); err != nil {
				t.Service.LogWithFields(WarnLevel, ei.M{"type": "reply_to_accept"}, "could not open received pipeId (%s): %s", repPath, err.Error())
			} else if _, err = pipe.Write(map[string]interface{}{"result": res, "error": errm, "task": map[string]interface{}{"path": t.Path, "method": t.Method, "params": t.Params, "tags": t.Tags}}); err != nil {
				t.Service.LogWithFields(WarnLevel, ei.M{"type": "reply_to_accept"}, "error writing response to pipe: %s", err.Error())
			}
		} else if repTy == "service" {
			if _, err := t.GetConn().TaskPush(repPath, map[string]interface{}{"result": res, "error": errm, "task": map[string]interface{}{"path": t.Path, "method": t.Method, "params": t.Params, "tags": t.Tags}}, time.Second*30, &nexus.TaskOpts{Detach: true}); err != nil {
				t.Service.LogWithFields(WarnLevel, ei.M{"type": "reply_to_accept"}, "could not push response task to received path (%s): %s", repPath, err.Error())
			}
		}
		return res, errm
	}
}

/*
AddMethod adds (or replaces if already added) a method for the service.
If three arguments are provided, the third is a struct containing options for the method.
The function that receives the nexus.Task should return a result or an error.
*/
func (s *Service) AddMethod(name string, f func(*Task) (interface{}, *JsonRpcErr), opts ...*MethodOpts) {
	if len(opts) == 0 {
		opts = []*MethodOpts{&MethodOpts{}}
	}
	s.addMethod(name, nil, f, opts[0])
}

/*
SetHandler sets the task handler for all methods, to allow custom parsing of the method.
When a handler is set, methods added with AddMethod() have no effect.
Passing a nil will remove the handler and turn back to methods from AddMethod().
*/
func (s *Service) SetHandler(h func(*Task) (interface{}, *JsonRpcErr), opts ...*MethodOpts) {
	if h == nil {
		s.handler = nil
		return
	}
	if len(opts) == 0 {
		opts = []*MethodOpts{&MethodOpts{}}
	}
	opt := opts[0]
	s.handler = &method{disablePullLog: opt.DisablePullLog, enableResponseResultLog: opt.EnableResponseResultLog, enableResponseErrorLog: opt.EnableResponseErrorLog, logOnTimeExceeded: opt.LogOnTimeExceeded, f: defMethodWrapper(h)}
	if opt.TestFunction != nil {
		s.handler.testf = defMethodWrapper(opt.TestFunction)
	}
}

// SetDescription modifies the service description
func (s *Service) SetDescription(descr string) {
	s.Description = descr
}

// SetUrl modifies the service url
func (s *Service) SetUrl(url string) {
	s.Url = url
}

// SetUser modifies the service user
func (s *Service) SetUser(user string) {
	s.User = user
}

// SetPass modifies the service pass
func (s *Service) SetPass(pass string) {
	s.Pass = pass
}

// SetPrefix modifies the service path
func (s *Service) SetPath(path string) {
	s.Path = path
}

// SetPulls modifies the number of concurrent nexus task.pull calls
func (s *Service) SetPulls(pulls int) {
	s.Pulls = pulls
}

// SetMaxThreads modifies the number of maximum concurrent goroutines resolving nexus.Task
func (s *Service) SetMaxThreads(maxThreads int) {
	s.MaxThreads = maxThreads
}

// SetPullTimeout modifies the time to wait for a nexus.Task for each nexus.TaskPull call
func (s *Service) SetPullTimeout(t time.Duration) {
	s.PullTimeout = t
}

// SetLogLevel modifies the service log level (one of debug, info, warn, error, fatal, panic).
func (s *Service) SetLogLevel(t string) {
	t = strings.ToLower(t)
	SetLogLevel(t)
	if GetLogLevel() == t {
		s.LogLevel = t
		s.debugEnabled = t == "debug"
	}
}

// SetStatsPeriod changes the period for the stats to be printed
func (s *Service) SetStatsPeriod(t time.Duration) {
	s.StatsPeriod = t
}

// SetGratefulExitTime sets the gracefull waiting time after a call to StopGraceful() is done
func (s *Service) SetGracefulExit(t time.Duration) {
	s.GracefulExit = t
}

// SetVersion sets the version of the service
func (s *Service) SetVersion(major int, minor int, patch int) {
	s.Version = fmt.Sprintf("%d.%d.%d", major, minor, patch)
}

// SetTesting turns on or off the service testing mode
func (s *Service) SetTesting(t bool) {
	s.Testing = t
}

// IsTesting returns whether the service is in testing mode
func (s *Service) IsTesting() bool {
	return s.Testing
}

// GetMethods returns a list of the methods the service has
// It returns nil if using a handler
func (s *Service) GetMethods() []string {
	if s.handler != nil {
		return nil
	}
	ms := []string{}
	for m, _ := range s.methods {
		ms = append(ms, m)
	}
	return ms
}

// GracefulStop stops pulling tasks, tries to finish working tasks and then cancels nexus connection (with a timeout)
func (s *Service) GracefulStop() {
	select {
	case s.stopServeCh <- true:
	default:
	}
}

// Stop cancels nexus connection, cancelling all tasks and stops serving
func (s *Service) Stop() {
	select {
	case s.stopServeCh <- false:
	default:
	}
}

func (s *Service) setState(state NexusConnState) {
	if hook := s.ConnState; hook != nil {
		hook(s.GetConn(), state)
	}
}

/*
Serve connects and authenticates with nexus, starts all the pulls and starts serving.
It returns any error with the service or the first error from one of its pulls.
A SIGINT will cause the service to start a graceful stop, if another SIGINT is received then a hard stop will be done.
*/
func (s *Service) Serve() error {
	parseFlags()
	var err error

	defer s.setState(StateStopped)
	defer s.setConn(nil)
	s.setState(StateInitializing)

	// Set log level
	s.SetLogLevel(s.LogLevel)

	// Return an error if no methods where added
	if s.methods == nil && s.handler == nil {
		err = fmt.Errorf("no methods to serve")
		s.LogWithFields(ErrorLevel, ei.M{"type": "no_methods"}, err.Error())
		return err
	}

	// Parse url
	if !s.sharedConn {
		_, err = url.Parse(s.Url)
		if err != nil {
			err = fmt.Errorf("invalid nexus url (%s): %s", s.Url, err.Error())
			s.LogWithFields(ErrorLevel, ei.M{"type": "invalid_url"}, err.Error())
			return err
		}
	}

	// Check service
	if s.MaxThreads < 0 {
		s.MaxThreads = 1
	}
	if s.Pulls < 0 {
		s.Pulls = 1
	}
	if s.MaxThreads < s.Pulls {
		s.MaxThreads = s.Pulls
	}
	if s.PullTimeout < 0 {
		s.PullTimeout = 0
	}
	if s.StatsPeriod < time.Millisecond*100 {
		if s.StatsPeriod < 0 {
			s.StatsPeriod = 0
		} else {
			s.StatsPeriod = time.Millisecond * 100
		}
	}
	if s.GracefulExit <= time.Second {
		s.GracefulExit = time.Second
	}
	if s.Version == "" {
		s.Version = "0.0.0"
	}

	// Allow to stop
	s.stopServeCh = make(chan (bool), 1)
	s.stopping = false
	s.stopLock = &sync.Mutex{}

	if !s.sharedConn {
		s.setState(StateConnecting)
		// Dial
		s.connLock.Lock()
		s.nc, err = nxcli.Dial(s.Url, nxcli.NewDialOptions())
		s.connLock.Unlock()
		if err != nil {
			if err == nxcli.ErrVersionIncompatible {
				s.LogWithFields(WarnLevel, ei.M{"type": "incompatible_version"}, "connecting to an incompatible version of nexus at (%s): client (%s) server (%s)", s.Url, nxcli.Version, s.nc.NexusVersion)
			} else {
				err = fmt.Errorf("can't connect to nexus server (%s): %s", s.Url, err.Error())
				s.LogWithFields(ErrorLevel, ei.M{"type": "connection_error"}, err.Error())
				return err
			}
		}

		s.setState(StateLoggingIn)

		// Login
		s.connLock.Lock()
		_, err = s.nc.Login(s.User, s.Pass)
		s.connLock.Unlock()
		if err != nil {
			err = fmt.Errorf("can't login to nexus server (%s) as (%s): %s", s.Url, s.User, err.Error())
			s.LogWithFields(ErrorLevel, ei.M{"type": "login_error"}, err.Error())
			return err
		}
		s.connId = s.nc.Id()
	}
	s.setState(StateServing)

	// Output
	s.LogWithFields(InfoLevel, s.logMap(), "%s", s)

	// Serve
	s.stats = &Stats{}
	s.wg = &sync.WaitGroup{}
	if s.threadsSem == nil {
		s.threadsSem = newSemaphore(s.MaxThreads)
	}
	for i := 1; i < s.Pulls+1; i++ {
		go s.taskPull(i)
	}

	// Wait for signals
	if !s.sharedConn {
		go func() {
			signalChan := make(chan os.Signal, 1)
			signal.Notify(signalChan, os.Interrupt)
			<-signalChan
			s.LogWithFields(DebugLevel, ei.M{"type": "graceful_requested"}, "received SIGINT: stop gracefuly")
			s.GracefulStop()
			<-signalChan
			s.LogWithFields(DebugLevel, ei.M{"type": "stop_requested"}, "received SIGINT again: stop")
			s.Stop()
		}()
	}

	// Wait until the nexus connection ends
	gracefulTimeout := &time.Timer{}
	wgDoneCh := make(chan (bool), 1)
	var statsTicker *time.Ticker
	if s.StatsPeriod > 0 {
		statsTicker = time.NewTicker(s.StatsPeriod)
	}
	var graceful bool
	for {
		select {
		case <-statsTicker.C:
			if s.debugEnabled {
				nst := s.GetStats()
				s.LogWithFields(DebugLevel, s.logStatsMap(), "stats: threads[ %d/%d ] task_pulls[ done=%d timeouts=%d ] tasks[ pulled=%d panic=%d errmethod=%d served=%d running=%d ]", s.threadsSem.Used(), s.threadsSem.Cap(), nst.TaskPullsDone, nst.TaskPullTimeouts, nst.TasksPulled, nst.TasksPanic, nst.TasksMethodNotFound, nst.TasksServed, nst.TasksRunning)
			}
		case graceful = <-s.stopServeCh: // Someone called Stop() or GracefulStop()
			if !graceful {
				s.setStopping()
				s.nc.Close()
				gracefulTimeout = time.NewTimer(time.Second)
				continue
			}
			if !s.isStopping() {
				s.setStopping()
				gracefulTimeout = time.NewTimer(s.GracefulExit)
				go func() {
					s.wg.Wait()
					wgDoneCh <- true
				}()
			}
		case <-wgDoneCh: // All workers finished
			s.nc.Close()
			continue
		case <-gracefulTimeout.C: // Graceful timeout
			if !graceful {
				s.LogWithFields(DebugLevel, ei.M{"type": "stop"}, "stop: done")
				return nil
			}
			s.nc.Close()
			err = fmt.Errorf("graceful: timeout after %s", s.GracefulExit.String())
			s.LogWithFields(ErrorLevel, ei.M{"type": "graceful_timeout"}, err.Error())
			return err
		case <-s.nc.GetContext().Done(): // Nexus connection ended
			if s.isStopping() {
				if graceful {
					s.LogWithFields(DebugLevel, ei.M{"type": "graceful"}, "graceful: done")
				} else {
					s.LogWithFields(DebugLevel, ei.M{"type": "stop"}, "stop: done")
				}
				return nil
			}
			if ctxErr := s.nc.GetContext().Err(); ctxErr != nil {
				err = fmt.Errorf("stop: nexus connection ended: %s", ctxErr.Error())
				s.LogWithFields(ErrorLevel, ei.M{"type": "connection_ended"}, err.Error())
				return err
			}
			err = fmt.Errorf("stop: nexus connection ended: stopped serving")
			s.LogWithFields(ErrorLevel, ei.M{"type": "connection_ended"}, err.Error())
			return err
		}
	}
	return nil
}

func (s *Service) taskPull(n int) {
	for {
		// Exit if stopping serve
		if s.isStopping() {
			return
		}
		s.threadsSem.Acquire()
		if s.isStopping() {
			s.threadsSem.Release()
			return
		}

		// Make a task pull
		atomic.AddUint64(&s.stats.TaskPullsDone, 1)
		task, err := s.nc.TaskPull(s.Path, s.PullTimeout)
		if err != nil {
			if IsNexusErrCode(err, ErrTimeout) { // A timeout ocurred: pull again
				atomic.AddUint64(&s.stats.TaskPullTimeouts, 1)
				s.threadsSem.Release()
				continue
			}
			if !s.isStopping() && !IsNexusErrCode(err, ErrConnClosed) { // An error ocurred (bypass if cancelled because service stop)
				s.LogWithFields(ErrorLevel, ei.M{"type": "pull_error"}, "pull %d: pulling task: %s", n, err.Error())
				s.nc.Close()
			}
			s.threadsSem.Release()
			return
		}

		// A task has been pulled
		atomic.AddUint64(&s.stats.TasksPulled, 1)
		wtask := &Task{Task: *task, Service: s}

		// Get method or global handler
		m := s.handler
		if m == nil {
			var ok bool
			m, ok = s.methods[wtask.Method]
			if !ok { // Method not found
				_, err = wtask.SendErrorCtx(wtask.Ctx, ErrMethodNotFound, "", nil)
				if err != nil {
					s.LogWithFields(ErrorLevel, ei.M{"type": "send_error", "where": fmt.Sprintf("%s%s", wtask.Path, wtask.Method)}, "Could not send error: %s", err.Error())
				}
				atomic.AddUint64(&s.stats.TasksMethodNotFound, 1)
				s.threadsSem.Release()
				continue
			}
		}

		// Log pull
		if !m.disablePullLog {
			s.LogWithFields(InfoLevel, ei.M{"type": "pull", "path": wtask.Path, "method": wtask.Method, "params": wtask.Params, "user": wtask.User}, "pull %d: task[ path=%s method=%s user=%s params=%+v ]", n, wtask.Path, wtask.Method, wtask.User, wtask.Params)
		}

		// Execute the task
		go func() {
			defer s.threadsSem.Release()
			s.wg.Add(1)
			defer s.wg.Done()
			atomic.AddUint64(&s.stats.TasksRunning, 1)
			defer atomic.AddUint64(&s.stats.TasksRunning, ^uint64(0))

			// OTel: extract traceparent injected by the task pusher and start
			// a SERVER span that is a child of the pusher's CLIENT span.
			ctx := extractTraceparent(wtask.Params)
			ctx, span := startServerSpan(ctx, wtask.Method, wtask.Path)
			wtask.Ctx = ctx
			start := time.Now()
			defer func() {
				var taskErr error
				if e := wtask.Tags["@local-response-error"]; e != nil {
					if re, ok := e.(*JsonRpcErr); ok {
						taskErr = re
					}
				}
				recordServerRPCCall(ctx, start, wtask.Method, wtask.Path, taskErr)
				endServerSpan(span, taskErr)
			}()

			defer func() {
				if r := recover(); r != nil {
					var nerr error
					var ok bool
					atomic.AddUint64(&s.stats.TasksPanic, 1)
					nerr, ok = r.(error)
					if !ok {
						nerr = fmt.Errorf("pkg: %v", r)
					}
					stck := debug.Stack()
					s.LogWithFields(ErrorLevel, ei.M{"type": "task_exception"}, "pull %d: panic serving task: %s: %s", n, nerr.Error(), stck)
					_, err = wtask.SendErrorCtx(wtask.Ctx, ErrInternal, fmt.Sprintf("%s: %s", nerr.Error(), stck), nil)
					if err != nil {
						s.LogWithFields(ErrorLevel, ei.M{"type": "send_error", "where": fmt.Sprintf("%s%s", wtask.Path, wtask.Method)}, "Could not send error: %s", err.Error())
					}
				}
			}()

			// Pact: return mock
			metadata := ei.N(wtask.Params).M("@metadata").MapStrZ()
			if ei.N(metadata).M("pact").BoolZ() {
				for _, pact := range m.pacts {
					if pactm, err := ei.N(pact.input).MapStr(); err == nil {
						pactm["@metadata"] = metadata
						if reflect.DeepEqual(pactm, wtask.Params) {
							_, err = wtask.SendResultCtx(wtask.Ctx, pact.output)
							if err != nil {
								s.LogWithFields(ErrorLevel, ei.M{"type": "send_error", "where": fmt.Sprintf("%s%s", wtask.Path, wtask.Method)}, "Could not send error: %s", err.Error())
							}
							atomic.AddUint64(&s.stats.TasksServed, 1)
							return
						}
					}
				}
				_, err = wtask.SendErrorCtx(wtask.Ctx, ErrPactNotDefined, ErrStr[ErrPactNotDefined], nil)
				if err != nil {
					s.LogWithFields(ErrorLevel, ei.M{"type": "send_error", "where": fmt.Sprintf("%s%s", wtask.Path, wtask.Method)}, "Could not send error: %s", err.Error())
				}
				atomic.AddUint64(&s.stats.TasksServed, 1)
				return
			}

			// Validate input schema
			if m.inSchema != nil {
				result, err := m.inSchema.validator.Validate(gojsonschema.NewGoLoader(wtask.Params))
				if err != nil { // Error with schemas
					_, err = wtask.SendErrorCtx(wtask.Ctx, ErrInvalidParams, fmt.Sprintf("jsonschema validation failed: %s", err.Error()), nil)
					if err != nil {
						s.LogWithFields(ErrorLevel, ei.M{"type": "send_error", "where": fmt.Sprintf("%s%s", wtask.Path, wtask.Method)}, "Could not send error: %s", err.Error())
					}
					atomic.AddUint64(&s.stats.TasksServed, 1)
					return
				} else if !result.Valid() { // Schema validation error
					out := fmt.Sprintf("jsonschema validation failed: %s", schemaValidationErr(result))
					_, err = wtask.SendErrorCtx(wtask.Ctx, ErrInvalidParams, out, nil)
					if err != nil {
						s.LogWithFields(ErrorLevel, ei.M{"type": "send_error", "where": fmt.Sprintf("%s%s", wtask.Path, wtask.Method)}, "Could not send error: %s", err.Error())
					}
					atomic.AddUint64(&s.stats.TasksServed, 1)
					return
				}
			}

			// Execute the task
			started := time.Now()
			if ei.N(wtask.Params).M("@metadata").M("testing").BoolZ() {
				if m.testf == nil {
					_, err = wtask.SendErrorCtx(wtask.Ctx, ErrTestingMethodNotProvided, ErrStr[ErrTestingMethodNotProvided], nil)
					if err != nil {
						s.LogWithFields(ErrorLevel, ei.M{"type": "send_error", "where": fmt.Sprintf("%s%s", wtask.Path, wtask.Method)}, "Could not send error: %s", err.Error())
					}
					atomic.AddUint64(&s.stats.TasksServed, 1)
					return
				} else {
					m.testf(wtask)
				}
			} else {
				m.f(wtask)
			}

			took := time.Since(started)
			if m.logOnTimeExceeded > 0 && took > m.logOnTimeExceeded {
				s.LogWithFields(InfoLevel, ei.M{"type": "task_time_exceeded", "path": wtask.Path, "method": wtask.Method, "params": wtask.Params, "user": wtask.User, "took": took, "log_on_time_exceeded": m.logOnTimeExceeded}, "task took too much (%v > %v): task[ path=%s method=%s user=%s params=%+v ]", took, m.logOnTimeExceeded, wtask.Path, wtask.Method, wtask.User, wtask.Params)
			}

			// Log response
			if m.enableResponseResultLog && wtask.Tags["@local-response-result"] != nil {
				s.LogWithFields(InfoLevel, ei.M{"type": "response_result", "path": wtask.Path, "method": wtask.Method, "user": wtask.User, "params": wtask.Params, "result": wtask.Tags["@local-response-result"], "took": took}, "pull %d: task[ path=%s method=%s user=%s result=%+v ]", n, wtask.Path, wtask.Method, wtask.User, wtask.Tags["@local-response-result"])
			}
			if m.enableResponseErrorLog && wtask.Tags["@local-response-error"] != nil {
				s.LogWithFields(InfoLevel, ei.M{"type": "response_error", "path": wtask.Path, "method": wtask.Method, "user": wtask.User, "params": wtask.Params, "error": wtask.Tags["@local-response-error"], "took": took}, "pull %d: task[ path=%s method=%s user=%s error=%+v ]", n, wtask.Path, wtask.Method, wtask.User, wtask.Tags["@local-response-error"])
			}

			// Validate result schema
			if m.resSchema != nil && wtask.Tags["@local-response-result"] != nil {
				result, err := m.resSchema.validator.Validate(gojsonschema.NewGoLoader(wtask.Tags["@local-response-result"]))
				if err != nil {
					s.LogWithFields(ErrorLevel, ei.M{"type": "output_schema_error"}, "jsonschema result validation failed: %s", err.Error())
				} else if !result.Valid() {
					s.LogWithFields(ErrorLevel, ei.M{"type": "output_schema_error"}, "jsonschema result validation failed: %s", schemaValidationErr(result))
				}
			}

			// Validate error schema
			if m.errSchema != nil && wtask.Tags["@local-response-error"] != nil {
				result, err := m.errSchema.validator.Validate(gojsonschema.NewGoLoader(wtask.Tags["@local-response-error"]))
				if err != nil {
					s.LogWithFields(ErrorLevel, ei.M{"type": "output_schema_error"}, "jsonschema error validation failed: %s", err.Error())
				} else if !result.Valid() {
					s.LogWithFields(ErrorLevel, ei.M{"type": "output_schema_error"}, "jsonschema error validation failed: %s", schemaValidationErr(result))
				}
			}

			atomic.AddUint64(&s.stats.TasksServed, 1)
		}()
	}
}

// GetStats returns the service stats
func (s *Service) GetStats() *Stats {
	return &Stats{
		TaskPullsDone:       atomic.LoadUint64(&s.stats.TaskPullsDone),
		TaskPullTimeouts:    atomic.LoadUint64(&s.stats.TaskPullTimeouts),
		TasksMethodNotFound: atomic.LoadUint64(&s.stats.TasksMethodNotFound),
		TasksPanic:          atomic.LoadUint64(&s.stats.TasksPanic),
		TasksPulled:         atomic.LoadUint64(&s.stats.TasksPulled),
		TasksServed:         atomic.LoadUint64(&s.stats.TasksServed),
		TasksRunning:        atomic.LoadUint64(&s.stats.TasksRunning),
	}
}

// Log allows to log from the service with the default format used by nxsugar
func (s *Service) Log(level string, message string, args ...interface{}) {
	fields := map[string]interface{}{}
	connid := s.getConnid()
	if connid != "" {
		fields["connid"] = connid
	}
	LogWithFields(level, s.Name+"/"+s.Path, fields, message, args...)
}

// LogWithFields allows to log from the service with the default format used by nxsugar adding some custom fields
func (s *Service) LogWithFields(level string, fields map[string]interface{}, message string, args ...interface{}) {
	if fields == nil {
		fields = map[string]interface{}{}
	}
	connid := s.getConnid()
	if connid != "" {
		fields["connid"] = connid
	}
	LogWithFields(level, s.Name+"/"+s.Path, fields, message, args...)
}

// String returns some service info as a stirng
func (s *Service) String() string {
	if s.sharedConn {
		return fmt.Sprintf("config: url=%s user=%s connid=%s version=%s path=%s pulls=%d pullTimeout=%s maxThreads=%d logLevel=%s statsPeriod=%s gracefulExit=%s", s.Url, s.User, s.getConnid(), s.Version, s.Path, s.Pulls, s.PullTimeout.String(), s.MaxThreads, s.LogLevel, s.StatsPeriod.String(), s.GracefulExit.String())
	}
	return fmt.Sprintf("config: url=%s user=%s connid=%s version=%s path=%s pulls=%d pullTimeout=%s maxThreads=%d logLevel=%s statsPeriod=%s gracefulExit=%s", s.Url, s.User, s.getConnid(), s.Version, s.Path, s.Pulls, s.PullTimeout.String(), s.MaxThreads, s.LogLevel, s.StatsPeriod.String(), s.GracefulExit.String())
}

func (s *Service) logMap() map[string]interface{} {
	return ei.M{
		"type":         "start",
		"url":          s.Url,
		"user":         s.User,
		"connid":       s.getConnid(),
		"version":      s.Version,
		"path":         s.Path,
		"pulls":        s.Pulls,
		"pullTimeout":  s.PullTimeout.String(),
		"maxThreads":   s.MaxThreads,
		"logLevel":     s.LogLevel,
		"statsPeriod":  s.StatsPeriod.String(),
		"gracefulExit": s.GracefulExit.String(),
	}
}

func (s *Service) logStatsMap() map[string]interface{} {
	nst := s.GetStats()
	return ei.M{
		"threadsUsed":         s.threadsSem.Used(),
		"threadsMax":          s.threadsSem.Cap(),
		"taskPullsDone":       nst.TaskPullsDone,
		"taskPullTimeouts":    nst.TaskPullTimeouts,
		"tasksPulled":         nst.TasksPulled,
		"tasksPanic":          nst.TasksPanic,
		"tasksMethodNotFound": nst.TasksMethodNotFound,
		"tasksServed":         nst.TasksServed,
		"tasksRunning":        nst.TasksRunning,
	}
}

func (s *Service) isStopping() bool {
	s.stopLock.Lock()
	defer s.stopLock.Unlock()
	return s.stopping
}

func (s *Service) setStopping() {
	s.stopLock.Lock()
	defer s.stopLock.Unlock()
	s.stopping = true
}
