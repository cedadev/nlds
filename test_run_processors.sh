#! /usr/bin/env bash
if [[ ! -d ~/nlds_log ]]
then
    mkdir ~/nlds_log
fi

echo "Running NLDS processors, <CTRL-C> to exit"

source ~/nlds-venv/bin/activate; python -u nlds_processors/nlds_worker.py > ~/nlds_log/nlds_worker.log &
echo "    Running nlds_worker with PID "${!}
source ~/nlds-venv/bin/activate; python -u nlds_processors/index.py > ~/nlds_log/index.log &
echo "    Running index with PID "${!}
source ~/nlds-venv/bin/activate; python -u nlds_processors/transfer.py > ~/nlds_log/transfer.log &
echo "    Running transfer with PID "${!}
source ~/nlds-venv/bin/activate; python -u nlds_processors/monitor.py > ~/nlds_log/monitor.log &
echo "    Running monitor with PID "${!}
source ~/nlds-venv/bin/activate; python -u nlds_processors/catalog.py > ~/nlds_log/catalog.log &
echo "    Running monitor with PID "${!}

# kill all sub processors on CTRL-C
trap 'kill 0' SIGINT;

wait