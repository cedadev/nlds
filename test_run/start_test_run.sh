#! /usr/bin/env bash
if [[ ! -d ~/nlds_log ]]
then
    mkdir ~/nlds_log
fi

source $HOME/python-venvs/nlds-venv/bin/activate
# start a named screen session
screen -S nlds -c test_run.rc 
