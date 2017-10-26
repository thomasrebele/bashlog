#!/bin/bash

if [ "$JAVA" == "" ]; then
	JAVA="java"
fi

HEAPSPACE=-Xmx8g
if [ $(hostname) = "elvis" ]; then 
	HEAPSPACE=-Xmx200g
fi

HEAPDUMP="-XX:+HeapDumpOnOutOfMemoryError"
MAXDISKCACHE=-Dstorage.diskCache.bufferSize=8192
OPT="$HEAPSPACE $HEAPDUMP"

remove-color() {
	sed "s/[[:cntrl:]]\[[0-9;]\{0,4\}m//g"
}

CPDIR=exec/
CP=$(echo -e "$(cat $CPDIR/classpath-additional.info):$(cat $CPDIR/classpath.info-$HOSTNAME)" | tr "\\n" ":")
CP=$(eval echo $CP)

if [ "$1" == "update-classpath" ]; then
	echo "update classpath"
	mvn dependency:build-classpath | remove-color | grep -v "\[INFO\]" | grep -v "\[WARN" > $CPDIR/classpath.info-$HOSTNAME
elif [ "$1" == "show-classpath" ]; then
	echo $CP
else
	if [ "$1" == "hotswap" ]; then
		OPT="$OPT -XXaltjvm=dcevm -javaagent:$HOME/software/java/addons/hotswap-agent.jar=autoHotswap=true $MAXDISKCACHE $HEAPSPACE"
		shift 1
	else
		OPT="$OPT"
	fi
	#echo "java -cp $(. cp.sh) $*"
	$JAVA $OPT -cp $CP:target/classes $*
fi
