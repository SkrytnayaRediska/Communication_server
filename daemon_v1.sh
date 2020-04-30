#!/bin/bash
program="kursach_2kurs_2sem.py"

case "$1" in
	start)
		python /home/parallels/PycharmProjects/comm_server/kursach_2kurs_2sem.py &
		PID=`ps -aef | grep "$program" | grep -v grep | awk '{print $2}'`
		disown %1
		;;
	stop)
		PID=`ps -aef | grep "$program" | grep -v grep | awk '{print $2}'`
		PI=$(pgrep -f kursach_2kurs_2sem.py)
		kill $PI
		;;
	help)
		echo "Use only start, stop or help commands"
		;;
	*)
		echo "Error"
		;;
esac
exit 0
