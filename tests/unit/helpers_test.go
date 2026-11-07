package unit

import (
	"encoding/json"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/models"
)

// mustJSON marshals v and fails the test on error.
func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}

// awaitFrame waits up to timeout for the next encoded frame on a connection.
func awaitFrame(t *testing.T, conn *gateway.Connection, timeout time.Duration) models.Frame {
	t.Helper()
	select {
	case raw := <-conn.Send():
		var frame models.Frame
		require.NoError(t, json.Unmarshal(raw, &frame))
		return frame
	case <-time.After(timeout):
		t.Fatal("expected a frame on the connection")
		return models.Frame{}
	}
}

// gaugeValue reads the current value of a gauge child.
func gaugeValue(t *testing.T, gauge interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, gauge.Write(metric))
	return metric.GetGauge().GetValue()
}
