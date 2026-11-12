# Cluster schedule

Laptop A's cluster is shared by the team and its resources are limited. Book
a run before starting a full-data job.

1. Post in the team project channel with the operator, date, start/end time
   (local Calgary time), laptop(s) needed, data/output paths, and an estimate
   of CPU, memory, and HDFS use.
2. Check the existing schedule and wait for confirmation from the cluster
   owners (Jubayer for Laptop A; Navjot for Laptop B). Do not assume a slot is
   reserved until the owners confirm.
3. Keep the reservation to the agreed window. At the end, record command,
   commit, start/end time, exit status, output locations, and resource issues
   in the associated GitHub issue; notify the next user and leave the cluster
   in the agreed state.
4. If a run will overrun or needs more resources, ask the next booked user
   before continuing. Critical checkpoints and shared HDFS maintenance take
   precedence only after notifying affected users.
5. Never read or overwrite another run's output directory. Give each run a
   unique output path and clean up only files you own, with owner approval for
   shared HDFS data.

| Date (Calgary) | Time | Operator | Laptop(s) | Work / output path | Status |
|---|---|---|---|---|---|
| | | | | | |

Use the table as a short-term view and post detailed reservations in the team
channel so changes are visible to everyone.
