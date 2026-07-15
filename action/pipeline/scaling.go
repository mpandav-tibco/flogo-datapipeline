package pipeline

import (
	"context"
	"fmt"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/scaling"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// runScaled executes the pipeline across logical partitions of the source for
// the "sharded" and "distributed" scaling modes. The per-partition work is
// delegated to drain (the same read→transform→write closure used by single
// mode), so transforms, checkpointing, watermarks and DLQ behave identically —
// the only difference is WHICH slice of the source each call processes and how
// partitions are assigned to this instance.
//
//	sharded     Static assignment: this instance owns partitions where
//	            index % instanceCount == instanceIndex. No coordinator needed;
//	            instances run independently (e.g. a Kubernetes StatefulSet).
//	distributed Dynamic assignment via a shared work queue (Postgres). Every
//	            instance competes to claim partitions; failed partitions are
//	            re-queued up to maxRetries. Best for autoscaled Deployments.
//
// When the configured source does not support partitioning (only jdbc and the
// object-store sources do today), the engine logs a warning and falls back to a
// single full-source drain so the run still succeeds.
func (a *DataPipelineAction) runScaled(
	ctx context.Context,
	runLog corelog.Logger,
	def *Definition,
	scMode string,
	src source.Source,
	_ map[string]interface{},
	runID string,
	drain func(pSrc source.Source, ckptKey string) error,
) error {
	psrc, ok := src.(source.PartitionableSource)
	if !ok {
		runLog.Warnf("scaling.mode=%s requested but source type %q is not partitionable — falling back to single-instance execution", scMode, def.Source.Type)
		return drain(src, runID)
	}

	// Resolve instance identity from config, overridable by INSTANCE_INDEX /
	// INSTANCE_COUNT env vars for Kubernetes StatefulSet deployments.
	cfg := scaling.Config{
		Mode:               scaling.Mode(scMode),
		InstanceIndex:      def.Scaling.InstanceIndex,
		InstanceCount:      def.Scaling.InstanceCount,
		PartitionStrategy:  def.Scaling.PartitionStrategy,
		PartitionsPerClaim: def.Scaling.PartitionsPerClaim,
	}
	cfg.ResolveFromEnv()

	targetCount := cfg.InstanceCount
	if targetCount < 1 {
		targetCount = 1
	}
	parts, err := psrc.Partitions(targetCount)
	if err != nil {
		return fmt.Errorf("scaling: enumerate partitions: %w", err)
	}
	if len(parts) == 0 {
		runLog.Warnf("scaling.mode=%s — source produced no partitions; nothing to do", scMode)
		return nil
	}

	// processPartition opens a fresh Source scoped to one partition and drains
	// it. The checkpoint key is namespaced by partition ID so each partition
	// resumes independently after a crash.
	processPartition := func(ctx context.Context, p source.Partition) error {
		ps, oerr := psrc.OpenPartition(p)
		if oerr != nil {
			return fmt.Errorf("scaling: open partition %s: %w", p.ID, oerr)
		}
		defer ps.Close()
		return drain(ps, runID+":"+p.ID)
	}

	switch scMode {
	case "sharded":
		mine := scaling.ShardedPartitions(parts, cfg.InstanceIndex, cfg.InstanceCount)
		runLog.Infof("scaling.mode=sharded — instance %d/%d owns %d of %d partition(s)",
			cfg.InstanceIndex, cfg.InstanceCount, len(mine), len(parts))
		for _, p := range mine {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := processPartition(ctx, p); err != nil {
				return err
			}
		}
		return nil

	case "distributed":
		if def.Scaling.Coordinator == nil {
			return fmt.Errorf("scaling.mode=distributed requires a coordinator config")
		}
		coord := def.Scaling.Coordinator
		if coord.Type != "postgres" {
			// Only the Postgres work queue is implemented today. Redis is
			// reserved (documented in the workqueue package) but not yet built,
			// so fail loudly instead of silently mis-scheduling work.
			return fmt.Errorf("scaling: coordinator type %q is not supported (use 'postgres')", coord.Type)
		}
		qcfg := scaling.Config{
			Mode:             scaling.ModeDistributed,
			CoordinatorType:  "postgres",
			CoordinatorDSN:   coord.DSN,
			LeaseSeconds:     coord.LeaseSeconds,
			HeartbeatSeconds: coord.HeartbeatSeconds,
			MaxRetries:       coord.MaxRetries,
		}
		queue, err := scaling.BuildQueue(qcfg, a.ref)
		if err != nil {
			return fmt.Errorf("scaling: build work queue: %w", err)
		}
		defer queue.Close()

		c := scaling.NewCoordinator(qcfg, queue)
		runLog.Infof("scaling.mode=distributed — enqueuing %d partition(s) to postgres coordinator", len(parts))
		if err := c.Enqueue(ctx, parts); err != nil {
			return fmt.Errorf("scaling: enqueue partitions: %w", err)
		}
		if err := c.ClaimLoop(ctx, processPartition); err != nil {
			return fmt.Errorf("scaling: claim loop: %w", err)
		}
		if err := c.WaitForCompletion(ctx); err != nil {
			return fmt.Errorf("scaling: wait for completion: %w", err)
		}
		if stats, serr := c.Stats(ctx); serr == nil {
			runLog.Infof("scaling.mode=distributed — queue stats: %v", stats)
		}
		return nil

	default:
		return drain(src, runID)
	}
}
