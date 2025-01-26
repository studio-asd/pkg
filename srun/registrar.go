package srun

var (
	_ (ServiceRegistrar) = (*registrarInitAware)(nil)
	_ (ServiceRegistrar) = (*registrarRunnerAware)(nil)
)

type registrarInitAware struct {
	s []Service
}

func (r *registrarInitAware) name() string {
	return "init_aware_registrar"
}

func (r *registrarInitAware) register(services ...ServiceInitAware) {
	for _, svc := range services {
		s := svc.(Service)
		r.s = append(r.s, s)
	}
}

func (r *registrarInitAware) services() []Service {
	return r.s
}

type registrarRunnerAware struct {
	s []Service
}

func (r *registrarRunnerAware) name() string {
	return "runner_aware_registrar"
}

func (r *registrarRunnerAware) register(services ...ServiceRunnerAware) {
	for _, svc := range services {
		s := svc.(Service)
		r.s = append(r.s, s)
	}
}

func (r *registrarRunnerAware) services() []Service {
	return r.s
}

func RegisterInitAwareServices(services ...ServiceInitAware) *registrarInitAware {
	r := &registrarInitAware{}
	r.register(services...)
	return r
}

func RegisterRunnerAwareServices(services ...ServiceRunnerAware) *registrarRunnerAware {
	r := &registrarRunnerAware{}
	r.register(services...)
	return r
}
