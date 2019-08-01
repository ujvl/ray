from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import random
import time
import json

import numpy as np

import ray
from ray.experimental.sgd.sgd_worker import SGDWorker
from ray.experimental.sgd.param_server import ParameterServer
from ray.experimental.sgd.allreduce import CheckpointableRingAllReduceWorker
from ray.experimental.sgd.allreduce import RingAllReduceWorker
from ray.experimental.sgd.allreduce import CHECKPOINT_DIR

logger = logging.getLogger(__name__)


class DistributedSGD(object):
    """Experimental distributed SGD implementation in Ray.

    This supports two modes:
        'simple': centralized gradient aggregation
        'ps': sharded parameter-server implementation

    To use this class, you'll have to implement model.py:Model.

    Arguments:
        model_creator (func): Function that returns a model given worker and
            device indexes as arguments. Each model replica will be created
            within its own variable scope.
        num_workers (int): Number of Ray actors to use for SGD.
        devices_per_worker (int): Number of GPU or CPU devices to use per
            worker. One model replica will be created per device.
        gpu (bool): Whether to use GPU devices.
        strategy (str): Strategy to use for distributed gradient aggregation.
            This only applies if num_workers > 1.
        grad_shard_bytes (int): Fuse gradient tensors into chunks of at most
            this size (if applicable).
        all_reduce_alg (str): TensorFlow strategy to use for gradient
            synchronization within the same worker (if applicable).
            See modified_allreduce.py for options.

    Examples:
        >>> # Setup distributed SGD
        >>> model_creator = (
        ...   lambda worker_idx, device_idx: YourModelClass(...))
        >>> sgd = DistributedSGD(
        ...   model_creator, num_workers=2,
        ...   devices_per_worker=4, gpu=True, strategy="ps")

        >>> # To train
        >>> for i in range(100):
        ...   stats = sgd.step(fetch_stats=i % 10 == 0)

        >>> # To access or update model state
        >>> sgd.foreach_model(lambda model: ...)

        >>> # To access or update worker state
        >>> sgd.foreach_worker(lambda worker: ...)
    """

    def __init__(self,
                 model_creator,
                 num_workers,
                 devices_per_worker,
                 gpu=True,
                 strategy="ps",
                 grad_shard_bytes=10000000,
                 all_reduce_alg="simple",
                 node_resources=None,
                 checkpoint_interval=-1):
        # Create the checkpoint directory.
        self.checkpoint_dir = os.path.join(
            CHECKPOINT_DIR, ray.worker.global_worker.task_driver_id.hex())
        try:
            os.mkdir(CHECKPOINT_DIR)
        except FileExistsError:
            pass
        os.mkdir(self.checkpoint_dir)

        self.pinned = []


        if num_workers == 1 and strategy == "ps":
            logger.warning(
                "The parameter server strategy does not make sense for single "
                "worker operation, falling back to simple mode.")
            strategy = "simple"

        if strategy == "ps" or strategy == "allreduce":
            use_plasma_op = True
        elif strategy == "simple":
            use_plasma_op = False
            grad_shard_bytes = 0  # tensor fusion doesn't make sense
        else:
            raise ValueError("strategy must be one of 'ps', 'simple'")
        self.strategy = strategy

        # Pin constructor arguments.
        model_creator_id = ray.put((model_creator, np.zeros(1)))
        print("Pinning argument", model_creator_id)
        self.pinned.append(ray.get(model_creator_id))

        if gpu:
            requests = {"num_gpus": devices_per_worker}
        else:
            requests = {"num_cpus": devices_per_worker}

        self.workers = []
        logger.info(
            "Creating SGD workers ({} total, {} devices per worker)".format(
                num_workers, devices_per_worker))

        start = time.time()
        logger.info("Waiting for gradient configuration")
        RemoteSGDWorker = ray.remote(**requests)(SGDWorker)
        test_worker = RemoteSGDWorker.remote(
                    0,
                    model_creator_id,
                    num_devices=devices_per_worker,
                    plasma_op=use_plasma_op,
                    gpu=gpu,
                    max_bytes=grad_shard_bytes,
                    all_reduce_alg=all_reduce_alg,
                    checkpoint_dir=self.checkpoint_dir,
                    checkpoint_interval=checkpoint_interval)
        shard_shapes = ray.get(test_worker.shard_shapes.remote())
        logger.info("Got gradient configuration after %f", time.time() - start)
        del test_worker

        # ps_compute_apply returns the loss, then one return
        # value per gradient.
        num_return_vals = len(shard_shapes) + 1
        class SGDWorkerWithReturnValues(SGDWorker,
                ray.actor.Checkpointable):
            @ray.method(num_return_vals=num_return_vals)
            def ps_compute_apply(self,
                         agg_grad_shard_oids,
                         tl_name="ps_compute_apply",
                         write_timeline=False,
                         fetch_shards=False):
                return super(SGDWorkerWithReturnValues, self).ps_compute_apply(
                         agg_grad_shard_oids,
                         tl_name=tl_name,
                         write_timeline=write_timeline,
                         fetch_shards=fetch_shards)

        for worker_index in range(num_workers):
            if node_resources is not None:
                actor_resources = {node_resources[worker_index]: 1}
                requests["resources"] = actor_resources
                # TODO: pass checkpoint into ray.remote.
            requests["max_reconstructions"] = 100
            RemoteSGDWorker = ray.remote(**requests)(SGDWorkerWithReturnValues)
            self.workers.append(
                RemoteSGDWorker.remote(
                    worker_index,
                    model_creator_id,
                    num_devices=devices_per_worker,
                    plasma_op=use_plasma_op,
                    gpu=gpu,
                    max_bytes=grad_shard_bytes,
                    all_reduce_alg=all_reduce_alg,
                    checkpoint_dir=self.checkpoint_dir,
                    checkpoint_interval=checkpoint_interval))

        start = time.time()
        logger.info("Waiting for actors to start")
        ray.get([w.shard_shapes.remote() for w in self.workers])
        logger.info("Actors started after %f", time.time() - start)

        if strategy == "ps":
            logger.info("Starting parameter servers ({} shards)".format(
                len(shard_shapes)))
            self.ps_list = [
                ParameterServer.remote(len(self.workers), i)
                for i, s in enumerate(shard_shapes)
            ]
            ray.get([
                ps.initialize.remote(s)
                for ps, s in zip(self.ps_list, shard_shapes)
            ])
            logger.info("Parameter servers started")
        elif strategy == "allreduce":
            logger.info("Starting allreduce workers ({} shards)".format(
                len(shard_shapes)))

            # Create the allreduce workers.
            self.ps_list = [list() for _ in shard_shapes]
            in_place = False
            for worker_index in range(num_workers):
                for shard_index, s in enumerate(shard_shapes):
                    # Pin constructor arguments.
                    s_id = ray.put((s, np.zeros(1)))
                    print("Pinning argument", s_id)
                    self.pinned.append(ray.get(s_id))

                    actor_resources = {node_resources[worker_index]: 1}
                    cls = ray.remote(
                        resources=actor_resources,
                        max_reconstructions=100)(CheckpointableRingAllReduceWorker)
                    worker = cls.remote(
                            worker_index,
                            num_workers,
                            s_id,
                            in_place,
                            self.checkpoint_dir,
                            checkpoint_interval)
                    self.ps_list[shard_index].append(worker)
            # Exchange actor handles.
            for allreduce_workers in self.ps_list:
                for i in range(num_workers):
                    receiver_index = (i + 1) % num_workers
                    allreduce_workers[i].add_remote_worker.remote(
                            receiver_index, allreduce_workers[receiver_index])
                logger.info("Waiting for CheckpointableRingAllReduceWorkers to start")
                ray.get([w.ip.remote() for w in allreduce_workers])

        else:
            self.ps_list = []

    def foreach_worker(self, fn):
        """Apply the given function to each remote worker.

        Returns:
            List of results from applying the function.
        """
        results = ray.get([w.foreach_worker.remote(fn) for w in self.workers])
        return results

    def foreach_model(self, fn):
        """Apply the given function to each model replica in each worker.

        Returns:
            List of results from applying the function.
        """

        results = ray.get([w.foreach_model.remote(fn) for w in self.workers])
        out = []
        for r in results:
            out.extend(r)
        return out

    def for_model(self, fn):
        """Apply the given function to a single model replica.

        Returns:
            Result from applying the function.
        """
        return ray.get(self.workers[0].for_model.remote(fn))

    def step(self, fetch_stats=False, test_failure=False, kill_node_fn=None, num_failed=None):
        """Run a single SGD step.

        Arguments:
            fetch_stats (bool): Whether to return stats from the step. This can
                slow down the computation by acting as a global barrier.
        """
        if self.strategy == "ps":
            return _distributed_sgd_step(
                self.workers,
                self.ps_list,
                write_timeline=False,
                fetch_stats=fetch_stats)
        elif self.strategy == "allreduce":
            return _distributed_sgd_allreduce_step(
                self.workers,
                self.ps_list,
                write_timeline=False,
                fetch_stats=fetch_stats,
                test_failure=test_failure,
                kill_node_fn=kill_node_fn,
                num_failed=num_failed)
        else:
            return _simple_sgd_step(self.workers)

    def warmup(self):
        logger.info("Warming up object store of worker actors")
        ray.get([w.warmup.remote() for w in self.workers])
        logger.info("Warmup complete")

    def save_checkpoint(self, path):
        w0 = self.for_model(lambda m: m.get_weights())
        filename = os.path.join(path, "model.npy")
        np.save(filename, w0)

    def restore_checkpoint(self, path):
        filename = os.path.join(path, "model.npy")
        w0 = np.load(filename)
        self.foreach_model(lambda m: m.set_weights(w0))


def _average_gradients(grads):
    out = []
    for grad_list in zip(*grads):
        out.append(np.mean(grad_list, axis=0))
    return out


def _simple_sgd_step(actors):
    if len(actors) == 1:
        return {"loss": ray.get(actors[0].compute_apply.remote())}

    start = time.time()
    fetches = ray.get([a.compute_gradients.remote() for a in actors])
    losses = [f[0] for f in fetches]
    grads = [f[1] for f in fetches]
    logger.debug("compute all grads time {}".format(time.time() - start))
    start = time.time()
    if len(actors) == 1:
        assert len(grads) == 1
        avg_grad = grads[0]
    else:
        avg_grad = _average_gradients(grads)
        logger.debug("grad reduce time {}".format(time.time() - start))
    start = time.time()
    ray.get([a.apply_gradients.remote(avg_grad) for a in actors])
    logger.debug("apply all grads time {}".format(time.time() - start))
    return {"loss": np.mean(losses)}


def _distributed_sgd_step(actors, ps_list, fetch_stats, write_timeline):
    # Preallocate object ids that param servers will write new weights to
    accum_shard_ids = [np.random.bytes(20) for _ in ps_list]
    logger.debug("Generated accum oids")

    # Kick off the fused compute grad / update weights tf run for each actor
    losses = []
    grad_shard_oids_list = []
    for actor in actors:
        returns = actor.ps_compute_apply.remote(
                accum_shard_ids,
                write_timeline=write_timeline,
                fetch_shards=True)
        losses.append(returns.pop(0))
        grad_shard_oids_list.append(returns)
    logger.debug("Launched all ps_compute_applys on all actors")

    # Issue prefetch ops
    for j, (ps, weight_shard_oid) in list(
            enumerate(zip(ps_list, accum_shard_ids)))[::-1]:
        to_fetch = []
        for grad_shard_oids in grad_shard_oids_list:
            to_fetch.append(grad_shard_oids[j].binary())
        random.shuffle(to_fetch)
        ps.prefetch.remote(to_fetch)
    logger.debug("Launched all prefetch ops")

    # Aggregate the gradients produced by the actors. These operations
    # run concurrently with the actor methods above.
    ps_gets = []
    for j, (ps, weight_shard_oid) in list(
            enumerate(zip(ps_list, accum_shard_ids)))[::-1]:
        ps.add_spinwait.remote([gs[j].binary() for gs in grad_shard_oids_list])
        ps_gets.append(ps.get.remote(weight_shard_oid))
    logger.debug("Launched all aggregate ops")

    if write_timeline:
        timelines = [ps.get_timeline.remote() for ps in ps_list]
        logger.debug("Launched timeline gets")
        timelines = ray.get(timelines)
        t0 = timelines[0]
        for t in timelines[1:]:
            t0.merge(t)
        t0.chrome_trace_format("ps_timeline.json")
    else:
        # Wait for at least the ps gets to finish
        ray.get(ps_gets)

    returns = {
            "num_failed": 0,
            }
    if fetch_stats:
        returns["loss"] = np.mean(ray.get(losses))
    return returns


def _distributed_sgd_allreduce_step(actors, allreduce_workers_by_shard, fetch_stats, write_timeline, test_failure, kill_node_fn, num_failed):
    # Preallocate object ids that allreduce will write new weights to
    out_shard_ids_per_actor = [
        [np.random.bytes(20) for _ in allreduce_workers_by_shard]
        for _ in actors
    ]
    logger.debug("Generated accum oids")

    # Preallocate done oids to mark allreduce end
    done_ids_per_actor = [
        [np.random.bytes(20) for _ in allreduce_workers_by_shard]
        for _ in actors
    ]

    # Kick off the fused compute grad / update weights tf run for each actor
    losses = []
    grad_shard_oids_list = []
    for i, actor in enumerate(actors):
        returns = actor.ps_compute_apply.remote(
                out_shard_ids_per_actor[i],
                write_timeline=write_timeline,
                fetch_shards=False)
        losses.append(returns.pop(0))
        grad_shard_oids_list.append(returns)
    logger.debug("Launched all ps_compute_applys on all actors")

    # Issue allreduce ops
    for j in range(len(allreduce_workers_by_shard)):
        for i, a in enumerate(allreduce_workers_by_shard[j]):
            a.execute.remote(
                grad_shard_oids_list[i][j],
                done_ids_per_actor[i][j],
                out_shard_ids_per_actor[i][j])
    print("Launched all allreduce ops")

    # If we are testing locally with failures on, kill a worker halfway
    # through.
    if test_failure:
        time_to_sleep = np.random.rand() * 0.3
        time.sleep(time_to_sleep)
        kill_node_fn()

    if write_timeline:
        timelines = [ps.get_timeline.remote() for ps in ps_list]
        logger.debug("Launched timeline gets")
        timelines = ray.get(timelines)
        t0 = timelines[0]
        for t in timelines[1:]:
            t0.merge(t)
        t0.chrome_trace_format("ps_timeline.json")
    else:
        # Wait for at least the allreduce ops to finish
        allreduce_ops = []
        for done_ids in done_ids_per_actor:
            for d in done_ids:
                allreduce_ops.append(ray.ObjectID(d))

        # Suppress reconstruction since these object IDs were generated
        # out-of-band.
        timeout_s = 0.1
        iteration = 0
        while True:
            try:
                all_output_oids = ray.get(
                    allreduce_ops, suppress_reconstruction=True, timeout=timeout_s)
                break
            except ray.exceptions.RayGetTimeoutError:
                clients = ray.global_state.client_table()
                failed = len([client for client in clients if not client['IsInsertion']]) > 0
                if failed > num_failed:
                    print("Sending heartbeats...")
                    num_failed = failed
                    heartbeats = [
                            a.heartbeat.remote() for allreduce_workers in
                            allreduce_workers_by_shard for a in
                            allreduce_workers
                            ]
                    ray.get(heartbeats)
                    print("Done with heartbeats")

                    #worker_cursors = [a._ray_actor_cursor for a in actors]
                    #ray.wait(worker_cursors, num_returns=len(worker_cursors), timeout=0)
                    #print("Done with heartbeats")

    returns = {
            "num_failed": num_failed,
            }
    if fetch_stats:
        returns["loss"] = np.mean(ray.get(losses))
    return returns
