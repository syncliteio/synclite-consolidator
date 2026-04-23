#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

JVM_ARGS=""
if [ -x "$SCRIPT_DIR/synclite-variables.sh" ]; then
  source $SCRIPT_DIR/synclite-variables.sh
fi

if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA_CMD="$JAVA_HOME/bin/java"
else
  JAVA_CMD="java"
fi

"$JAVA_CMD" $JVM_ARGS -Djava.library.path="$SCRIPT_DIR/native" -classpath "$SCRIPT_DIR/synclite-consolidator.jar:$SCRIPT_DIR/lib/*" com.synclite.consolidator.Main $1 $2 $3 $4 $5 $6 $7
