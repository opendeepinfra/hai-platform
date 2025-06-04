ACTIVATE = '''
#!/usr/bin/env bash


deactivate () {
    # unset
    conda deactivate 2> /dev/null
    conda deactivate 2> /dev/null
    unset CMD
    unset CMD_LEN
    unset VENV
    unset PY_MAJ
    unset VENV_ROOT
    unset VENV_PATH

    if ! [[ -z "${_OLD_VENV_PATH+_}" ]]; then
        PATH="$_OLD_VENV_PATH"
        export PATH
        unset _OLD_VENV_PATH
    fi

    if ! [[ -z "${_OLD_HF_ENV_NAME+_}" ]]; then
        HF_ENV_NAME="$_OLD_HF_ENV_NAME"
        export HF_ENV_NAME
        unset _OLD_HF_ENV_NAME
    fi

    if ! [[ -z "${_OLD_HF_ENV_OWNER+_}" ]]; then
        HF_ENV_OWNER="$_OLD_HF_ENV_OWNER"
        export HF_ENV_OWNER
        unset _OLD_HF_ENV_OWNER
    fi

    if ! [[ -z "${_OLD_PYTHON_PATH+_}" ]]; then
        PYTHONPATH="$_OLD_PYTHON_PATH"
        export PYTHONPATH
        unset _OLD_PYTHON_PATH
    fi

    if ! [[ -z "${_OLD_PS1+_}" ]]; then
        PS1="$_OLD_PS1"
        export PS1
        unset _OLD_PS1
    fi

    if ! [[ -z "${_OLD_PIP_CONFIG_FILE+_}" ]]; then
        PIP_CONFIG_FILE="$_OLD_PIP_CONFIG_FILE"
        export PIP_CONFIG_FILE
        if [ ! ${PIP_CONFIG_FILE} ]; then
            unset PIP_CONFIG_FILE
        fi
        unset _OLD_PIP_CONFIG_FILE
    fi

    if ! [[ -z "${_OLD_PYTHONUSERBASE+_}" ]]; then
        PYTHONUSERBASE="$_OLD_PYTHONUSERBASE"
        export PYTHONUSERBASE
        if [ ! ${PYTHONUSERBASE} ]; then
            unset PYTHONUSERBASE
        fi
        unset _OLD_PYTHONUSERBASE
    fi

    if [[ ! "${1-}" = "nondestructive" ]]; then
    # Self destruct!
        unset -f deactivate
    fi
}

deactivate nondestructive

_OLD_VENV_PATH="${PATH}"
__EXTEND_HF_ENV__
export PATH

_OLD_PYTHON_PATH="${PYTHONPATH}"
PYTHONPATH="/opt/alpha-lib-linux/build/pyarmor_3"

declare -a alphapath=(
    "/opt/alpha-lib-linux/build/out/lib"
    "/opt/alpha-lib-linux/home"
    "/opt/alpha-lib-linux/home/share"
    "/opt/alpha-lib-linux/home/share/store_tools"
    "/opt/apex"
    "/opt/alpha_packs"
)

for i in "${alphapath[@]}"
do
    PYTHONPATH="${PYTHONPATH}:${i}"
done

PYTHONPATH=".:${PYTHONPATH}"
export PYTHONPATH

_OLD_HF_ENV_NAME="${HF_ENV_NAME}"
HF_ENV_NAME=__HF_ENV_NAME__
export HF_ENV_NAME

_OLD_HF_ENV_OWNER="${HF_ENV_OWNER}"
HF_ENV_OWNER=__HF_ENV_OWNER__
export HF_ENV_OWNER

_OLD_PS1="${PS1-}"
PS1="__NAME__ ${PS1-}"
export PS1

_OLD_PIP_CONFIG_FILE="${PIP_CONFIG_FILE}"
PIP_CONFIG_FILE="__PIP_PATH__"
export PIP_CONFIG_FILE

_OLD_PYTHONUSERBASE="${PYTHONUSERBASE}"
PYTHONUSERBASE="__PATH__"
export PYTHONUSERBASE

conda config --set changeps1 false
if (($?)); then
    echo "conda有问题，请检查conda或是换个目录重试"
    deactivate nondestructive
    return -1
fi
__conda_setup="$('conda' 'shell.bash' 'hook' 2> /dev/null)"
eval "$__conda_setup"
unset __conda_setup

basepath=$(cd `dirname $BASH_SOURCE`; pwd)
conda activate ${basepath}
unset basepath
echo "user venv __NAME__ loaded"
'''
