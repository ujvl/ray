#ifndef RAY_RAYLET_WORKER_H
#define RAY_RAYLET_WORKER_H

#include <memory>

#include "ray/common/client_connection.h"
#include "ray/id.h"
#include "ray/raylet/scheduling_resources.h"

namespace ray {

namespace raylet {

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
 public:
  /// A constructor that initializes a worker object.
  Worker(const WorkerID &worker_id, pid_t pid, const Language &language,
         std::shared_ptr<LocalClientConnection> connection);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  WorkerID WorkerId() const;
  void MarkDead();
  bool IsDead() const;
  void MarkBlocked();
  void MarkUnblocked();
  bool IsBlocked() const;
  /// Return the worker's PID.
  pid_t Pid() const;
  Language GetLanguage() const;
  void AssignTaskIds(const std::vector<TaskID> &task_ids);
  bool IsAssignedTaskId(const TaskID &task_id);
  const TaskID PopAssignedTaskID();
  const std::vector<TaskID> &GetAssignedTaskIds() const;
  bool AddBlockedTaskId(const TaskID &task_id);
  bool RemoveBlockedTaskId(const TaskID &task_id);
  const std::unordered_set<TaskID> &GetBlockedTaskIds() const;
  void AssignDriverId(const DriverID &driver_id);
  const DriverID &GetAssignedDriverId() const;
  void AssignActorId(const ActorID &actor_id);
  const ActorID &GetActorId() const;
  /// Return the worker's connection.
  const std::shared_ptr<LocalClientConnection> Connection() const;

  const ResourceIdSet &GetLifetimeResourceIds() const;
  void SetLifetimeResourceIds(ResourceIdSet &resource_ids);
  void ResetLifetimeResourceIds();

  const ResourceIdSet &GetTaskResourceIds() const;
  void SetTaskResourceIds(ResourceIdSet &resource_ids);
  void ResetTaskResourceIds();
  ResourceIdSet ReleaseTaskCpuResources();
  void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources);

 private:
  WorkerID worker_id_;
  /// The worker's PID.
  pid_t pid_;
  /// The language type of this worker.
  Language language_;
  /// Connection state of a worker.
  std::shared_ptr<LocalClientConnection> connection_;
  /// The worker's currently assigned task.
  std::vector<TaskID> assigned_task_ids_;
  /// Driver ID for the worker's current assigned task.
  DriverID assigned_driver_id_;
  /// The worker's actor ID. If this is nil, then the worker is not an actor.
  ActorID actor_id_;
  /// Whether the worker is dead.
  bool dead_;
  /// Whether the worker is blocked. Workers become blocked in a `ray.get`, if
  /// they require a data dependency while executing a task.
  bool blocked_;
  /// The specific resource IDs that this worker owns for its lifetime. This is
  /// only used for actors.
  ResourceIdSet lifetime_resource_ids_;
  /// The specific resource IDs that this worker currently owns for the duration
  // of a task.
  ResourceIdSet task_resource_ids_;
  std::unordered_set<TaskID> blocked_task_ids_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_H
