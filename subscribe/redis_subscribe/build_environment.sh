#!/bin/bash
#
# BEFORE EXECUTING THIS SCRIPT!
#
# Edit $VENV/bin/activate and add these 2 lines for the PYTHONPATH environment variable (change the path to your own project path)
#    PYTHONPATH="/home/linda/vsc-workspace-duke/pubsub/beanstalk_subscribe"
#    export PYTHONPATH
#
#
VENV=.venv
source $VENV/bin/activate
#
PYTHONPATH="/home/linda/vsc-workspace-duke/pubsub/beanstalk_subscribe"
export PYTHONPATH
#
pip install -r requirements.txt
