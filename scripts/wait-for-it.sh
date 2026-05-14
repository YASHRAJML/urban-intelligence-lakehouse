#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# wait-for-it.sh — Wait for a host:port to be available
# Source: https://github.com/vishnubob/wait-for-it (MIT License)
# ──────────────────────────────────────────────────────────────────────────────
cmdname=$(basename $0)

echoerr() { if [[ $QUIET -ne 1 ]]; then echo "$@" 1>&2; fi }

usage() {
    cat << USAGE >&2
Usage: $cmdname host:port [-s] [-t timeout] [-- command args]
    -h HOST | --host=HOST       Host or IP
    -p PORT | --port=PORT       TCP port
    -s | --strict               Exit 1 after timeout
    -q | --quiet                Do not output status messages
    -t TIMEOUT | --timeout=TIMEOUT  Timeout (seconds), zero=indefinite
    -- COMMAND ARGS             Execute command after test finishes
USAGE
    exit 1
}

wait_for() {
    if [[ $TIMEOUT -gt 0 ]]; then
        echoerr "$cmdname: waiting $TIMEOUT seconds for $HOST:$PORT"
    else
        echoerr "$cmdname: waiting for $HOST:$PORT without timeout"
    fi
    start_ts=$(date +%s)
    while :; do
        if [[ $ISBUSY -eq 1 ]]; then
            nc -z $HOST $PORT
            result=$?
        else
            (echo >/dev/tcp/$HOST/$PORT) >/dev/null 2>&1
            result=$?
        fi
        if [[ $result -eq 0 ]]; then
            end_ts=$(date +%s)
            echoerr "$cmdname: $HOST:$PORT is available after $((end_ts - start_ts)) seconds"
            break
        fi
        sleep 1
        if [[ $TIMEOUT -gt 0 ]]; then
            now_ts=$(date +%s)
            if [[ $((now_ts - start_ts)) -ge $TIMEOUT ]]; then
                echoerr "$cmdname: timeout after waiting $TIMEOUT seconds for $HOST:$PORT"
                if [[ $STRICT -eq 1 ]]; then
                    exit 1
                fi
                break
            fi
        fi
    done
    return $result
}

QUIET=0; STRICT=0; TIMEOUT=15; HOST=""; PORT=""; CMD=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        *:* ) hostport=(${1//:/ }); HOST=${hostport[0]}; PORT=${hostport[1]}; shift 1 ;;
        -q | --quiet) QUIET=1; shift 1 ;;
        -s | --strict) STRICT=1; shift 1 ;;
        -h) HOST="$2"; shift 2 ;;
        --host=*) HOST="${1#*=}"; shift 1 ;;
        -p) PORT="$2"; shift 2 ;;
        --port=*) PORT="${1#*=}"; shift 1 ;;
        -t) TIMEOUT="$2"; shift 2 ;;
        --timeout=*) TIMEOUT="${1#*=}"; shift 1 ;;
        --) shift; CMD="$@"; break ;;
        --help) usage ;;
        *) echoerr "Unknown argument: $1"; usage ;;
    esac
done

if [[ "$HOST" == "" || "$PORT" == "" ]]; then
    echoerr "Error: you need to provide a host and port to test."
    usage
fi

ISBUSY=0
nc -z $HOST $PORT 2>/dev/null; [[ $? -ne 0 ]] && ISBUSY=1
wait_for
RESULT=$?
if [[ $CMD != "" ]]; then
    if [[ $RESULT -ne 0 && $STRICT -eq 1 ]]; then
        echoerr "$cmdname: strict mode, refusing to execute subprocess"
        exit $RESULT
    fi
    exec $CMD
else
    exit $RESULT
fi
