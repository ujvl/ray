from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time
import os
import sys

import numpy as np
import pyarrow.plasma as plasma
import tensorflow as tf

import ray
from ray.experimental.sgd.util import (ensure_plasma_tensorflow_op, fetch,
                                       run_timeline, warmup)
from ray.experimental.sgd.modified_allreduce import (sum_gradients_all_reduce,
                                                     unpack_small_tensors)
import ray.cloudpickle as pickle

logger = logging.getLogger(__name__)


class SGDWorker(object):
    """Helper class for ray.experimental.sgd.DistributedSGD."""

    def __init__(self,
                 worker_index,
                 model_creator,
                 all_reduce_alg="simple",
                 num_devices=1,
                 gpu=False,
                 max_bytes=10000000,
                 plasma_op=False,
                 checkpoint_dir=None,
                 checkpoint_interval=-1):
        # The second entry in model_creator is just a dummy numpy array.
        model_creator, _ = model_creator

        self.worker_index = worker_index
        self.num_iterations = 0
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_interval = checkpoint_interval
        if self.checkpoint_dir is not None:
            try:
                os.makedirs(self.checkpoint_dir)
            except FileExistsError:
                pass
        self.checkpoint_attrs = [
            "checkpoint_dir",
            "worker_index",
            "num_iterations",
        ]
        self._should_checkpoint = False

        assert num_devices > 0

        # TODO(ekl) support custom session
        tf_session_args = {
            "device_count": {
                "CPU": num_devices
            },
            "log_device_placement": False,
            "gpu_options": tf.GPUOptions(force_gpu_compatible=True),
            "inter_op_parallelism_threads": 128,
        }
        config_proto = tf.ConfigProto(**tf_session_args)
        self.sess = tf.Session(config=config_proto)
        self.models = []
        grad_ops = []

        if gpu:
            device_tmpl = "/gpu:%d"
        else:
            device_tmpl = "/cpu:%d"
        with self.sess.as_default():
            for device_idx in range(num_devices):
                device = device_tmpl % device_idx
                with tf.device(device):
                    with tf.variable_scope("device_%d" % device_idx):
                        model = model_creator(worker_index, device_idx)
                        self.models.append(model)
                        optimizer = model.get_optimizer()
                        loss = model.get_loss()
                        grads = [
                            t for t in optimizer.compute_gradients(loss)
                            if t[0] is not None
                        ]
                        grad_ops.append(grads)

        if num_devices == 1:
            if max_bytes:
                raise ValueError(
                    "Implementation limitation: grad_shard_bytes > 0 "
                    "({}) currently requires > 1 device".format(max_bytes))
            self.packed_grads_and_vars = grad_ops
        else:
            if max_bytes:
                self.packed_grads_and_vars, packing_vals = (
                    sum_gradients_all_reduce(
                        "",
                        grad_ops,
                        1,
                        all_reduce_alg,
                        1,
                        list(range(num_devices)),
                        agg_small_grads_max_bytes=max_bytes))
            else:
                self.packed_grads_and_vars, _ = (sum_gradients_all_reduce(
                    "",
                    grad_ops,
                    1,
                    all_reduce_alg,
                    1,
                    list(range(num_devices)),
                    agg_small_grads_max_bytes=0))
        self.per_device_grads = [
            list(zip(*dev_gv))[0] for dev_gv in self.packed_grads_and_vars
        ]
        assert (len(self.per_device_grads) == num_devices)
        self.num_grads = num_grads = len(self.packed_grads_and_vars[0])
        if max_bytes:
            logger.info("Packed grads => {} tensors".format(num_grads))

        # Ops for reading grads with the right control deps
        nccl_noops = []
        for j in range(num_grads)[::-1]:
            deps = nccl_noops + [
                dev_grad[j] for dev_grad in self.per_device_grads
            ]
            with tf.control_dependencies(deps):
                nccl_noops = [tf.no_op()]

        # You must fetch this otherwise the NCCL allreduce will hang
        self.nccl_control_out = tf.group(*nccl_noops)

        if plasma_op:
            store_socket = (
                ray.worker.global_worker.plasma_client.store_socket_name)
            ensure_plasma_tensorflow_op()

            # For fetching grads -> plasma
            self.plasma_in_grads = []
            self.plasma_in_grads_oids = [
                tf.placeholder(shape=[], dtype=tf.string, name="in_grad_oids")
                for _ in range(num_grads)
            ]
            for j in range(num_grads):
                grad = self.per_device_grads[0][j]
                with tf.device(self.models[0].get_loss().device):
                    plasma_grad = plasma.tf_plasma_op.tensor_to_plasma(
                        [grad],
                        self.plasma_in_grads_oids[j],
                        plasma_store_socket_name=store_socket)
                self.plasma_in_grads.append(plasma_grad)

            # For applying grads <- plasma
            unpacked_gv = []
            self.plasma_out_grads_oids = [
                tf.placeholder(
                    shape=[], dtype=tf.string, name="grad_out_oids")
                for _ in range(num_grads)
            ]
            packed_plasma_grads = []
            for j in range(num_grads):
                # This control dependency is probably not needed.
                #with tf.device(self.plasma_in_grads[j].device):
                #    with tf.control_dependencies([self.plasma_in_grads[j]]):
                grad_ph = plasma.tf_plasma_op.plasma_to_tensor(
                    self.plasma_out_grads_oids[j],
                    dtype=tf.float32,
                    plasma_store_socket_name=store_socket)
                grad_ph = tf.reshape(grad_ph,
                                     self.packed_grads_and_vars[0][j][0].shape)
                logger.debug("Packed tensor {}".format(grad_ph))
                packed_plasma_grads.append(grad_ph)
            for i in range(num_devices):
                per_device = []
                for j, (g, v) in enumerate(self.packed_grads_and_vars[i]):
                    grad_ph = packed_plasma_grads[j]
                    per_device.append((grad_ph, v))
                unpacked_gv.append(per_device)

            if max_bytes:
                unpacked_gv = unpack_small_tensors(unpacked_gv, packing_vals)

        elif max_bytes:
            unpacked_gv = unpack_small_tensors(self.packed_grads_and_vars,
                                               packing_vals)
        else:
            unpacked_gv = self.packed_grads_and_vars

        # Same shape as packed_grads_and_vars
        assert len(unpacked_gv) == num_devices
        assert len(unpacked_gv[0][0]) == 2

        apply_ops = []
        to_apply = unpacked_gv[0]
        for ix, m in enumerate(self.models):
            apply_ops.append(m.get_optimizer().apply_gradients([
                (g, v) for ((g, _), (_, v)) in zip(to_apply, unpacked_gv[ix])
            ]))
        self.apply_op = tf.group(*apply_ops)
        init_op = tf.group(tf.global_variables_initializer(),
                           tf.local_variables_initializer())
        self.sess.run(init_op)
        self.saver = tf.train.Saver(tf.trainable_variables())

    def save_checkpoint(self, actor_id, checkpoint_id):
        logger.info("Saving checkpoint %d actor:%s checkpoint:%s", self.num_iterations, actor_id.hex(), checkpoint_id.hex())
        assert self.checkpoint_dir is not None
        with ray.profiling.profile("save_checkpoint"):
            checkpoint = {}
            for attr in self.checkpoint_attrs:
                checkpoint[attr] = getattr(self, attr)
            checkpoint_path = os.path.join(
                    self.checkpoint_dir, checkpoint_id.hex())
            checkpoint = pickle.dumps(checkpoint)
            with open(checkpoint_path, 'wb+') as f:
                f.write(checkpoint)

            with ray.profiling.profile("get_model"):
                with self.sess.graph.as_default():
                    checkpoint_path = os.path.join(
                            self.checkpoint_dir,
                            "{}-{}.npy".format(actor_id.hex(), self.num_iterations))
                    self.saver.save(self.sess, checkpoint_path, write_meta_graph=False, write_state=False)
            # Getting the weights out the first time is really slow for some
            # reason.
            #with ray.profiling.profile("get_model"):
            #    weights = self.for_model(lambda m: m.get_weights())
            #with ray.profiling.profile("save_model"):
            #    checkpoint_path = os.path.join(
            #            self.checkpoint_dir,
            #            "{}-{}.npy".format(actor_id.hex(), self.num_iterations))
            #    np.save(checkpoint_path, weights)
            #logger.info("Done saving checkpoint %d actor:%s checkpoint:%s model size:%d", self.num_iterations, actor_id.hex(), checkpoint_id.hex(), sys.getsizeof(weights))

    def load_checkpoint(self, actor_id, available_checkpoints):
        with ray.profiling.profile("restore_checkpoint"):
            checkpoint_id = available_checkpoints[0].checkpoint_id
            logger.info("Restoring checkpoint actor:%s checkpoint:%s", actor_id.hex(), checkpoint_id.hex())
            checkpoint_path = os.path.join(self.checkpoint_dir,
                                           checkpoint_id.hex())
            with open(checkpoint_path, 'rb') as f:
                checkpoint = pickle.loads(f.read())
            for attr in self.checkpoint_attrs:
                setattr(self, attr, checkpoint[attr])

            #with ray.profiling.profile("load_model"):
            #    checkpoint_path = os.path.join(
            #            self.checkpoint_dir,
            #            "{}-{}.npy".format(actor_id.hex(), self.num_iterations))
            #    weights = np.load(checkpoint_path)
            #with ray.profiling.profile("set_model"):
            #    self.foreach_model(lambda m: m.set_weights(weights))
            with ray.profiling.profile("load_model"):
                with self.sess.as_default():
                    checkpoint_path = os.path.join(
                            self.checkpoint_dir,
                            "{}-{}.npy".format(actor_id.hex(), self.num_iterations))
                    self.saver.restore(self.sess, checkpoint_path)

            logger.info("Done restoring checkpoint %d actor:%s checkpoint:%s", self.num_iterations, actor_id.hex(), checkpoint_id.hex())
            return checkpoint_id

    def checkpoint_expired(self, actor_id, checkpoint_id):
        pass

    def _grad_feed_dict(self):
        # Aggregate feed dicts for each model on this worker.
        feed_dict = {}
        for model in self.models:
            feed_dict.update(model.get_feed_dict())
        return feed_dict

    def foreach_model(self, fn):
        with self.sess.as_default():
            return [fn(m) for m in self.models]

    def foreach_worker(self, fn):
        with self.sess.as_default():
            return fn(self)

    def for_model(self, fn):
        with self.sess.as_default():
            return fn(self.models[0])

    def compute_gradients(self):
        start = time.time()
        feed_dict = self._grad_feed_dict()
        # We only need to fetch the first per_device_grad, since they are
        # averaged across all devices by allreduce.
        fetches = self.sess.run(
            [
                self.models[0].get_loss(), self.per_device_grads[0],
                self.nccl_control_out
            ],
            feed_dict=feed_dict)
        logger.debug(
            "Compute grad interior time {}".format(time.time() - start))
        return fetches

    def apply_gradients(self, avg_grads):
        start = time.time()
        result = {
            g: avg_grads[i]
            for (i, g) in enumerate(self.per_device_grads[0])
        }
        self.sess.run(self.apply_op, feed_dict=result)
        logger.debug("Apply grad interior time {}".format(time.time() - start))

    def compute_apply(self):
        fetches = run_timeline(
            self.sess,
            [self.models[0].get_loss(), self.apply_op, self.nccl_control_out],
            feed_dict=self._grad_feed_dict(),
            name="compute_apply")
        return fetches[0]

    def ps_compute_apply(self,
                         agg_grad_shard_oids,
                         tl_name="ps_compute_apply",
                         write_timeline=False,
                         fetch_shards=False):
        out_grad_shard_oids = [
                object_id.binary() for object_id in
                ray.worker.global_worker.skip_returns(1)
                ]
        [loss_id] = ray.worker.global_worker.skip_returns(0)
        agg_grad_shard_ids = [ray.ObjectID(oid) for oid in agg_grad_shard_oids]
        print("ps_compute_apply",
              self.num_iterations,
              [ray.ObjectID(oid) for oid in out_grad_shard_oids],
              agg_grad_shard_ids)

        feed_dict = self._grad_feed_dict()
        feed_dict.update(
            dict(zip(self.plasma_out_grads_oids, agg_grad_shard_oids)))
        feed_dict.update(
            dict(zip(self.plasma_in_grads_oids, out_grad_shard_oids)))
        loss = None

        _, lost = ray.wait(agg_grad_shard_ids, num_returns=len(agg_grad_shard_oids), timeout=0,
                           request_once=True)
        if lost:
            if fetch_shards:
                fetch(agg_grad_shard_oids)
            fetches = run_timeline(
                self.sess, [
                    self.models[0].get_loss(), self.plasma_in_grads, self.apply_op,
                    self.nccl_control_out
                ],
                feed_dict=feed_dict,
                write_timeline=write_timeline)
            loss = fetches[0]

        else:
            for oid in out_grad_shard_oids:
                oid = ray.ObjectID(oid)
                ray.worker.global_worker.put_object(oid, None)
            run_timeline(
                self.sess, [self.apply_op],
                feed_dict=feed_dict,
                write_timeline=write_timeline)

        # Store the loss now to avoid blocking on the checkpoint op.
        ray.worker.global_worker.put_object(loss_id, loss)

        # Determine whether we should take a checkpoint.
        logger.info("Done with iteration %d", self.num_iterations)
        self.num_iterations += 1
        if (self.checkpoint_interval > 0 and self.num_iterations %
                self.checkpoint_interval == 0):
            logger.info("Should checkpoint at %d", self.num_iterations)
            self._should_checkpoint = True

        # We have applied the gradients so it's safe to delete them.
        agg_grad_shard_ids = [ray.pyarrow.plasma.ObjectID(oid) for oid in agg_grad_shard_oids]
        ray.worker.global_worker.plasma_client.delete(agg_grad_shard_ids)


    def should_checkpoint(self, checkpoint_context):
        should_checkpoint = self._should_checkpoint
        self._should_checkpoint = False
        return should_checkpoint

    def num_grad_shards(self):
        return self.num_grads

    def shard_shapes(self):
        main_gv = self.packed_grads_and_vars[0]
        return [g.shape for g, _ in main_gv]

    def ip(self):
        return ray.services.get_node_ip_address()

    def warmup(self):
        warmup()
