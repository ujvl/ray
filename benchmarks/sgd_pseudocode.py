workers = [w1, w2, w3]
allreduce_workers = [[a11, a21, a31], [a12, a22, a32]]


def step():
    # Where the workers will write gradient shards to.
    out_grad_ids = [None] * len(workers)
    # Where the allreduce workers will write the aggregated updates to.
    agg_grad_ids = [None] * len(workers)

    # Submit the tensorflow ops.
    for i, worker in enumerate(workers):
        agg_grads[i] = []
        for allreducers in allreduce_workers:
            for _ in allreducers
                agg_grads[i].append(np.random.bytes())

        # Run the tensorflow session. Give it the object IDs to set as
        # placeholders.
        # NOTE: Suppress fetch for non-failures? Alternatively, submit a task
        # to aggregate first.
        # NOTE: Must send lineage of in_grads even though they are not explicit
        # dependencies.
        out_grad_ids[i] = worker.compute.remote(agg_grad_ids)

    # Submit the allreduces.
    for shard_index, shard_reducers in enumerate(allreduce_workers):
        # Launch an allreduce for every shard.
        for node_index, reducer in enumerate(shard_reducers):
            allreduce_worker.execute.remote(out_grad_ids[node_index][shard_index],
                                            done_oid=agg_grad_ids[node_index][shard_index])

    # Wait for the tensorflow ops to finish.
    # NOTE: We probably don't want the data.
    ray.get(out_grad_ids)


class Worker(object):

    @ray.remote(num_return_vals=num_shards)
    def compute(self, agg_grad_ids):
        # Set the output placeholders.
        # NOTE: Turn off task leases when we fake the return?
        for i in range(num_shards):
            tf.out_grad_placeholders[i] = ray.worker.get_return_value(i)

        # Set the placeholders with in_grads.
        tf.agg_grad_placeholders = agg_grad_ids

        # Start the fetch.
        ray.wait(agg_grad_ids, timeout=0)

        # Run the tf session.

        return ray.NoReturns()

class AllreduceWorker(object):
    def execute(self, input_data, done_oid):
        assert [oid is None for oid in self.out_oids]

        # All of the chunks should be aggregated and concatenated into this ID.
        self.done_oid = ray.worker.get_return_value(i)

        # Start the allreduce.

    def receive(self, data):
        # Process the data.

        # If done, store the final output.
        self_handle.finish.remote(*self.out_oids)

    # NOTE: Overwrite the execution dependency to be the actor creation object
    # once this task is ready to execute.
    @ray.reset_state
    def finish(self, *outputs):
        ray.put(outputs, self.done_oid)
        self.reset()
