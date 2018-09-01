# Run a shell command on all hosts.
#
# Environment Variables
#
#   SUCCINCT_HOSTS    File naming remote hosts.
#     Default is ${SUCCINCT_CONF_DIR}/hosts.
#   SUCCINCT_CONF_DIR  Alternate conf dir. Default is ${SUCCINCT_HOME}/conf.
#   SUCCINCT_HOST_SLEEP Seconds to sleep between spawning remote commands.
#   SUCCINCT_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: hosts.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
conf="`cd "$sbin/../conf/"; pwd`"

HOSTS="$conf/hosts"
if [ -f "$HOSTS" ]; then
  HOSTLIST=`cat "$HOSTS"`
fi

# By default disable strict host key checking
if [ "$SSH_OPTS" = "" ]; then
  SSH_OPTS="-o StrictHostKeyChecking=no -i ~/key.pem"
fi

FORMAT=false
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
    if [ "$FORMAT" = true ] ; then
        ssh $SSH_OPTS "$host" $"${@// /\\ }" \
            2>&1 | sed "s/^/$host: /" &
    else
        ssh $SSH_OPTS "$host" $"${@// /\\ }" 2>&1 &
    fi
done

wait
