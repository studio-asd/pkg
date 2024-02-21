// healthcheck provides active and passive healthchecks to the 'service' inside the runner.
//
// Active Healthcheck
//
// With active healthcheck, its possible for services to produce/push its health status and
// consumes other services health status. If the service somehow depends on the other service
// status and its affect on how the service is running, it then can set its own status to
// either degraded or unhealthy depends on the condition. The point of active healthcheck
// is to give an immediate notification and propagation for other services.
//
// |-------|
// | svc_1 | ----------                                |-------|
// |-------|          | push_notif            |------> | svc_2 |
//                    v                       |        |-------|
//               |-------------|              |
//               | Healthcheck |   broadcast  |        |-------|
//               |   Service   | --------------------> | svc_1 |
//               |-------------|              |        |-------|
//                    ^                       |
// |-------|          | push_notif            |        |-------|
// | svc_2 | ----------                       |------> | svc_3 |
// |-------|                                           |-------|
//
// Passive Healthcheck
//
// The passive healthcheck is a check that being done by the healthcheck service. It will asks
// other services of what are their status. All the status will also broascasted to the notifier
// so other services can also get the benefit of the status notification.

package srun

import (
	"context"
	"errors"
	"sync"
	"time"
)

type (
	HealthStatus       int
	HealthStatusSource int
)

const (
	// HealthStatusStopped can be set when the service is go into a stop state.
	HealthStatusStopped = iota + 1
	// HealthStatusUhealthy can be set when the service is not in a state to receive any request because
	// some internal state in the service is not behaving as expected.
	//
	// For example, if a database is not responding we should issue this status.
	HealthStatusUhealthy
	// HealthStatusDegraded can be set when the service is partially not available. For example, if a service is dependent
	// to another service for some part of the service, we can mark the service as degraded.
	HealthStatusDegarded
	// HealthStatusHealthy can be set when the service is ready to receive/serve requests.
	HealthStatusHealthy
)

const (
	// HealthStatusSourceCheckService means the source of the status update is coming from the healthcheck service.
	HealthStatusSourceCheckService = iota + 1
	// HealthStatusSourceCheckService means the source of the status update is coming from the healthcheck notifier or the service itself
	// push the check notification.
	HealthStatusSourceCheckNotifier
)

// String returns the state in string.
func (h HealthStatus) String() string {
	return []string{
		"UNKNOWN_STATUS",
		"STOPPED",
		"UNHEALTHY",
		"HEALTHY",
	}[h]
}

// String returns the state in string.
func (h HealthStatusSource) String() string {
	return []string{
		"UNKNOWN_SOURCE",
		"SERVICE",
		"NOTIFIER",
	}[h]
}

// Healthcheck checks whether the service is in a healthy condition or not. The check is not to be confused by ready check
// as healthcheck will always running after the service is ready. If the healthcheck fails, the runner will use the status
// for internal status and publish it to the service who needs the checks.
type Healthcheck interface {
	ServiceRunnerAware
	// Health check the service health and returns error if service is not healthy.
	// Context is passed so we have a healthcheck timeout and ensure the check is
	// not running forever.
	//
	// The health function returns two variables(status and error) to the checker.
	// If health status is unknown and error is not nil, then the service will be treated
	// as unhealthy. But if the status is returned, and error is not nill then the error
	// will be treated as information. Otherwise the health status will be used.
	Health(ctx context.Context) (HealthStatus, error)
}

// HealthcheckConsumer consumes the healthcheck messages from the healthcheck service. The check messages will be multiplexed
// to all services that consumes the notification.
//
// The point of this interface is to allow services to just implement ServiceRunnerAware. The package will automatically detect
// whether this interface is implemented or not and act accordingly.
type HealthcheckConsumer interface {
	ServiceRunnerAware
	// ConsumeHealthcheckNotification consumes all the healthcheck notifications from all services in the subscriptions.
	// The function provides filter to allow the client to list the services to listen to. The client will skips all the
	// messages from unrelated services and only pass the matched one to the function.
	//
	// For example:
	//	func (s *Service) ConsumeHealthcheckNotification(fn HealthcheckNotifyFunc) error {
	//		err := fn([]string{"service_1"}, func(notif HealthcheckNotification) error {
	//			switch notif.ServiceName {
	//				case "service_1":
	//					// Do something.
	//			}
	//		})
	//	}
	ConsumeHealthcheckNotification(HealthcheckNotifyFunc) error
}

// HealthcheckNotifyFunc manage the loops and notify the HealthcheckNotifyFunc all the notifications of the subscriptions.
// The reason of why this function exist is because we don't want for each servie to maintain its for loop. Its tedious
// and some of them might doing it wrong thing by not exiting the tight loop. So its better for the healthcheck service
// to maintain this process.
type HealthcheckNotifyFunc func([]string, HealthcheckConsumeFunc) error

// HealthcheckConsumeFunc consumes the healthcheck notification and loops through all the services healthcheck notification that
// the service subscribes to.
type HealthcheckConsumeFunc func(HealthcheckNotification) error

// HealthcheckNotification is a message of notification with service healthcheck status information.
type HealthcheckNotification struct {
	ServiceName    string
	Status         HealthStatus
	CheckTimestamp int64
	Source         HealthStatusSource
}

type HealthcheckConfig struct {
	Disable bool
	Timeout time.Duration
}

// HealthcheckService provides a service that actively checks the services. And it continuously notify other services
// that subscribes for any health updates from other services.
type HealthcheckService struct {
	config HealthcheckConfig
	notifC chan HealthcheckNotification
	// noifiers is the healthcheck notifier for all services. We provide the notifier for all services because ther might
	// be a service that won't consume the notification but they need to send the notification. For example a service that
	// held database resource might don't have a need to consume healthcheck notification but it need to send the notification
	// if in any case the database is down.
	notifiers map[ServiceRunnerAware]*HealthcheckNotifier
	// services is the list of services that need checks.
	services map[ServiceRunnerAware]Healthcheck
	// consumers is the consumers of the healthcheck notification. We are using a concurrent services to start the
	// all the consumers.
	consumers        []HealthcheckConsumer
	consumersService *ConcurrentServices
	// broadcastC is the healthcheck notification channel. The number of channel will be the same with the number of consumers
	// as this channel acted as the multiplexer to all consumers(all consumers will receive the same message). We don't create
	// the channel for all services because it is pointeless to provide the notification for a service without consumer.
	broadcastC []chan HealthcheckNotification
}

func newHealthcheckService(config HealthcheckConfig) *HealthcheckService {
	hcs := &HealthcheckService{
		config:    config,
		services:  make(map[ServiceRunnerAware]Healthcheck),
		notifiers: make(map[ServiceRunnerAware]*HealthcheckNotifier),
	}
	if !config.Disable {
		ccs, _ := newConcurrentServices(nil)
		hcs.consumersService = ccs
		hcs.notifC = make(chan HealthcheckNotification, 100)
	}
	return hcs
}

func (h *HealthcheckService) register(svc ServiceRunnerAware) error {
	// Check the type of the service and register recursively if the condition is met. The type switch here is needed
	// because we want to register the 'real' service type into the healthchecker, and not the wrapper inside the runner.
	switch real := svc.(type) {
	case *ConcurrentServices:
		// ConcurrentServices is tricky to register because it wraps several services into 'ConcurrentServices' where it
		// being wrapped further using 'ServiceStateTracker' to track their state. In this case, we need to ensure we
		// load all the services('ServiceStateTracker') inside it and loop over them.
		var err error
		for _, realSvc := range real.services {
			errRegister := h.register(realSvc)
			if errRegister != nil {
				err = errors.Join(err, errRegister)
			}
		}
		return err
	case *ServiceStateTracker:
		// ServiceStateTracker is the wrapper type inside the runner, so we should not use the type to register it to the healthchecker.
		// Instead, recursively register it to the HealthcheckService to register the real service.
		return h.register(real.ServiceRunnerAware)
	}

	hcf := &HealthcheckNotifier{
		serviceName: svc.Name(),
		noop:        h.config.Disable,
		notifyC:     h.notifC,
	}

	// Don't build or track anything else if the healthcheck is disabled.
	if !h.config.Disable {
		hc, ok := svc.(Healthcheck)
		if ok {
			h.services[svc] = hc
		}
		hcc, ok := svc.(HealthcheckConsumer)
		if ok {
			notifC := make(chan HealthcheckNotification, 100)
			h.consumersService.Register(svc)
			h.consumers = append(h.consumers, hcc)
			h.broadcastC = append(h.broadcastC, notifC)
		}
	}
	h.notifiers[svc] = hcf
	return nil
}

func (h *HealthcheckService) Run(ctx context.Context) error {
	// var notificationsC []chan HealthcheckNotification
	// // Create the consumers of health check notification.
	// for _, consumer := range h.consumers {
	// 	notifC := make(chan HealthcheckNotification, 100)
	// 	NewLongRunningTask(fmt.Sprint("consumer-healthcheck-%s", consumer.Name()), func(ctx context.Context) error {
	// 		return consumer.ConsumeHealthcheckNotification(func(s []string, hcf HealthcheckConsumeFunc) error {
	// 			return nil
	// 		})
	// 	})
	// 	notificationsC = append(notificationsC, notifC)
	// }
	return nil
}

func (h *HealthcheckService) Stop(ctx context.Context) error {
	return nil
}

// check directly triggers the healthcheck to the service. This function is exposed to the internal runner because sometimes we need to
// directly check the service.
func (h *HealthcheckService) check(ctx context.Context, svc ServiceRunnerAware) (HealthStatus, error) {
	return h.services[svc].Health(ctx)
}

// HealthcheckNotifier notifies the healthcheck service of a given service health status by pushing them. The health service then will
// multiplex the notification to all subscribers.
type HealthcheckNotifier struct {
	mu sync.RWMutex
	// noop means we will not send the notification to the channel. We need this mode because we don't want to return an empty notifier.
	// We can't afford the empty notifier because the user will have nil pointer panic if we do so.
	noop        bool
	serviceName string
	notifyC     chan<- HealthcheckNotification
}

// Notify sends the status of the service health to the health service via notification channel.
func (h *HealthcheckNotifier) Notify(status HealthStatus) {
	h.mu.RLock()
	// Don't send anything if we use noop. And noop will be enabled if healthcheck is disabled.
	if h.noop {
		h.mu.RUnlock()
		return
	}
	h.mu.RUnlock()

	h.notifyC <- HealthcheckNotification{
		ServiceName:    h.serviceName,
		Status:         status,
		CheckTimestamp: time.Now().UnixMilli(),
		Source:         HealthStatusSourceCheckNotifier,
	}
}

// stop stops the notifier by enabling the noop flag.
func (h *HealthcheckNotifier) stop() {
	h.mu.Lock()
	h.noop = true
	h.mu.Unlock()
}
