package demo

import (
	"database/sql"
	"time"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/promql"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// The real types satisfy the demo's interfaces as they are, with no adapter:
// a signature drift in any of them fails the build of this package's tests.
var (
	_ Orchestrator = (*orchestrator.Orchestrator)(nil)
	_ Experiments  = (*store.ExperimentRepo)(nil)
	_ Rules        = (*store.RuleRepo)(nil)
	_ Specs        = (*store.FaultRepo)(nil)
	_ Workflows    = (*store.WorkflowRepo)(nil)
	_ LoadGen      = (*zeus.Client)(nil)
	_ Instances    = (*atrocontrol.Controller)(nil)
	_ Metrics      = (*promql.Client)(nil)
	_ Pinger       = (*sql.DB)(nil)
	_ Clock        = ClockFunc(time.Now)
)
