package walops

import "go.uber.org/zap"

// StandaloneSameTerm is Standalone with the hard state's term kept, as
// etcd's --force-new-cluster keeps it. Tests use it as the control that
// shows why Standalone moves to the next term.
func StandaloneSameTerm(lg *zap.Logger, walDir string, self uint64, remove []uint64) (StandaloneResult, error) {
	return standalone(lg, walDir, self, remove, true)
}
