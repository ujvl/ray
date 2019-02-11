# Run a shell command on all hosts.

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

# if [ "$VENV" = "" ]; then
#   VENV="source ~/py-env/venv3/bin/activate"
# fi

for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
    if [ "$FORMAT" = "" ] ; then
        ssh $SSH_OPTS "$host" $"${@// /\\ }" 2>&1 &
    else
        ssh $SSH_OPTS "$host" $"${@// /\\ }" \
            2>&1 | sed "s/^/$host: /" &
    fi
done

wait
