package jobs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginAddWorker(t *testing.T) {
	t.Run("no pool configured", func(t *testing.T) {
		p := &Plugin{}

		require.Error(t, p.AddWorker())
	})

	t.Run("single pool", func(t *testing.T) {
		fp := &fakePool{}
		p := &Plugin{workersPool: fp}

		require.NoError(t, p.AddWorker())
		assert.Equal(t, 1, fp.added)
	})

	t.Run("every named pool", func(t *testing.T) {
		first, second := &fakePool{}, &fakePool{}
		p := &Plugin{workersPools: map[string]Pool{"default": first, "high": second}}

		require.NoError(t, p.AddWorker())
		assert.Equal(t, 1, first.added)
		assert.Equal(t, 1, second.added)
	})

	t.Run("pool error is returned", func(t *testing.T) {
		fp := &fakePool{addErr: errors.New("allocate failed")}
		p := &Plugin{workersPools: map[string]Pool{"default": fp}}

		require.ErrorIs(t, p.AddWorker(), fp.addErr)
	})
}

func TestPluginRemoveWorker(t *testing.T) {
	t.Run("no pool configured", func(t *testing.T) {
		p := &Plugin{}

		require.Error(t, p.RemoveWorker(t.Context()))
	})

	t.Run("single pool", func(t *testing.T) {
		fp := &fakePool{}
		p := &Plugin{workersPool: fp}

		require.NoError(t, p.RemoveWorker(t.Context()))
		assert.Equal(t, 1, fp.removed)
	})

	t.Run("every named pool", func(t *testing.T) {
		first, second := &fakePool{}, &fakePool{}
		p := &Plugin{workersPools: map[string]Pool{"default": first, "high": second}}

		require.NoError(t, p.RemoveWorker(t.Context()))
		assert.Equal(t, 1, first.removed)
		assert.Equal(t, 1, second.removed)
	})

	t.Run("pool error is returned", func(t *testing.T) {
		fp := &fakePool{removeErr: errors.New("destroy failed")}
		p := &Plugin{workersPools: map[string]Pool{"default": fp}}

		require.ErrorIs(t, p.RemoveWorker(t.Context()), fp.removeErr)
	})
}
