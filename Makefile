GCC = gcc -Wall -Wextra -Wpedantic -pthread -g
EXE = loadbalancer
${EXE}: loadbalancer.c
	${GCC} -o $@ $^
clean:
	rm -f ${EXE} *.o