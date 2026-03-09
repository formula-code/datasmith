# Snapshot file
# Unset all aliases to avoid conflicts with functions
# Functions
gawklibpath_append ()
{
    [ -z "$AWKLIBPATH" ] && AWKLIBPATH=`gawk 'BEGIN {print ENVIRON["AWKLIBPATH"]}'`;
    export AWKLIBPATH="$AWKLIBPATH:$*"
}
gawklibpath_default ()
{
    unset AWKLIBPATH;
    export AWKLIBPATH=`gawk 'BEGIN {print ENVIRON["AWKLIBPATH"]}'`
}
gawklibpath_prepend ()
{
    [ -z "$AWKLIBPATH" ] && AWKLIBPATH=`gawk 'BEGIN {print ENVIRON["AWKLIBPATH"]}'`;
    export AWKLIBPATH="$*:$AWKLIBPATH"
}
gawkpath_append ()
{
    [ -z "$AWKPATH" ] && AWKPATH=`gawk 'BEGIN {print ENVIRON["AWKPATH"]}'`;
    export AWKPATH="$AWKPATH:$*"
}
gawkpath_default ()
{
    unset AWKPATH;
    export AWKPATH=`gawk 'BEGIN {print ENVIRON["AWKPATH"]}'`
}
gawkpath_prepend ()
{
    [ -z "$AWKPATH" ] && AWKPATH=`gawk 'BEGIN {print ENVIRON["AWKPATH"]}'`;
    export AWKPATH="$*:$AWKPATH"
}

# setopts 3
set -o braceexpand
set -o hashall
set -o interactive-comments

# aliases 0

# exports 74
declare -x ANTIGRAVITY_CLI_ALIAS="agy"
declare -x BROWSER="/home/asehgal/.antigravity-server/bin/1.19.4-09171cd6214f9521f5202ffe72bda4443582da95/bin/helpers/browser.sh"
declare -x CLAUDE_CODE_TMPDIR="/home/asehgal/tmp/claude"
declare -x CODEX_HOME="/mnt/sdd1/atharvas/formulacode/datasmith_new/scratch/configs/codex_oss20b_config"
declare -x CODEX_MANAGED_BY_NPM="1"
declare -x COLORTERM="truecolor"
declare -x CONDA_DEFAULT_ENV="base"
declare -x CONDA_EXE="/mnt/sdd1/atharvas/env/miniconda3/bin/conda"
declare -x CONDA_JL_CONDA_EXE="/mnt/sdd1/atharvas/env/miniconda3/bin/conda"
declare -x CONDA_JL_CONDA_EXE_BACKUP=""
declare -x CONDA_JL_HOME="/mnt/sdd1/atharvas/env/miniconda3"
declare -x CONDA_JL_HOME_BACKUP=""
declare -x CONDA_PREFIX="/mnt/sdd1/atharvas/env/miniconda3"
declare -x CONDA_PROMPT_MODIFIER="(base) "
declare -x CONDA_PYTHON_EXE="/mnt/sdd1/atharvas/env/miniconda3/bin/python"
declare -x CONDA_SHLVL="1"
declare -x DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/1007/bus"
declare -x DISPLAY="localhost:10.0"
declare -x GEMINI_CLI_IDE_AUTH_TOKEN="d92d7da0-6575-43b9-b933-a7526765caa3"
declare -x GEMINI_CLI_IDE_SERVER_PORT="42227"
declare -x GEMINI_CLI_IDE_WORKSPACE_PATH="/mnt/sdd1/atharvas/formulacode/datasmith_new"
declare -x GIT_ASKPASS="/home/asehgal/.antigravity-server/bin/1.19.4-09171cd6214f9521f5202ffe72bda4443582da95/extensions/git/dist/askpass.sh"
declare -x HF_HOME="/mnt/sdd3/llama_atharvas/huggingface"
declare -x HOME="/home/asehgal"
declare -x JULIA_CONDAPKG_BACKEND="System"
declare -x JULIA_CONDAPKG_BACKEND_BACKUP=""
declare -x JULIA_CONDAPKG_EXE="/mnt/sdd1/atharvas/env/miniconda3/bin/conda"
declare -x JULIA_CONDAPKG_EXE_BACKUP=""
declare -x JULIA_DEPOT_PATH="/mnt/sdd1/atharvas/env/miniconda3/share/julia:/mnt/sdd1/atharvas/env/miniconda3/share/pysr/depot:"
declare -x JULIA_DEPOT_PATH_BACKUP=""
declare -x JULIA_DEPOT_PATH_PYSR_BACKUP="/mnt/sdd1/atharvas/env/miniconda3/share/julia:"
declare -x JULIA_LOAD_PATH="@:@miniconda3:@stdlib"
declare -x JULIA_LOAD_PATH_BACKUP=""
declare -x JULIA_PROJECT="@miniconda3"
declare -x JULIA_PROJECT_BACKUP=""
declare -x JULIA_SSL_CA_ROOTS_PATH="/mnt/sdd1/atharvas/env/miniconda3/ssl/cacert.pem"
declare -x JULIA_SSL_CA_ROOTS_PATH_BACKUP=""
declare -x LANG="en_US.UTF-8"
declare -x LD_LIBRARY_PATH=":/home/asehgal/julia-1.6.7/lib/:/home/asehgal/julia-1.6.7/lib/"
declare -x LESSCLOSE="/usr/bin/lesspipe %s %s"
declare -x LESSOPEN="| /usr/bin/lesspipe %s"
declare -x LOGNAME="asehgal"
declare -x LS_COLORS="rs=0:di=01;34:ln=01;36:mh=00:pi=40;33:so=01;35:do=01;35:bd=40;33;01:cd=40;33;01:or=40;31;01:mi=00:su=37;41:sg=30;43:ca=30;41:tw=30;42:ow=34;42:st=37;44:ex=01;32:*.tar=01;31:*.tgz=01;31:*.arc=01;31:*.arj=01;31:*.taz=01;31:*.lha=01;31:*.lz4=01;31:*.lzh=01;31:*.lzma=01;31:*.tlz=01;31:*.txz=01;31:*.tzo=01;31:*.t7z=01;31:*.zip=01;31:*.z=01;31:*.dz=01;31:*.gz=01;31:*.lrz=01;31:*.lz=01;31:*.lzo=01;31:*.xz=01;31:*.zst=01;31:*.tzst=01;31:*.bz2=01;31:*.bz=01;31:*.tbz=01;31:*.tbz2=01;31:*.tz=01;31:*.deb=01;31:*.rpm=01;31:*.jar=01;31:*.war=01;31:*.ear=01;31:*.sar=01;31:*.rar=01;31:*.alz=01;31:*.ace=01;31:*.zoo=01;31:*.cpio=01;31:*.7z=01;31:*.rz=01;31:*.cab=01;31:*.wim=01;31:*.swm=01;31:*.dwm=01;31:*.esd=01;31:*.jpg=01;35:*.jpeg=01;35:*.mjpg=01;35:*.mjpeg=01;35:*.gif=01;35:*.bmp=01;35:*.pbm=01;35:*.pgm=01;35:*.ppm=01;35:*.tga=01;35:*.xbm=01;35:*.xpm=01;35:*.tif=01;35:*.tiff=01;35:*.png=01;35:*.svg=01;35:*.svgz=01;35:*.mng=01;35:*.pcx=01;35:*.mov=01;35:*.mpg=01;35:*.mpeg=01;35:*.m2v=01;35:*.mkv=01;35:*.webm=01;35:*.webp=01;35:*.ogm=01;35:*.mp4=01;35:*.m4v=01;35:*.mp4v=01;35:*.vob=01;35:*.qt=01;35:*.nuv=01;35:*.wmv=01;35:*.asf=01;35:*.rm=01;35:*.rmvb=01;35:*.flc=01;35:*.avi=01;35:*.fli=01;35:*.flv=01;35:*.gl=01;35:*.dl=01;35:*.xcf=01;35:*.xwd=01;35:*.yuv=01;35:*.cgm=01;35:*.emf=01;35:*.ogv=01;35:*.ogx=01;35:*.aac=00;36:*.au=00;36:*.flac=00;36:*.m4a=00;36:*.mid=00;36:*.midi=00;36:*.mka=00;36:*.mp3=00;36:*.mpc=00;36:*.ogg=00;36:*.ra=00;36:*.wav=00;36:*.oga=00;36:*.opus=00;36:*.spx=00;36:*.xspf=00;36:"
declare -x MOTD_SHOWN="pam"
declare -x NVM_BIN="/home/asehgal/.nvm/versions/node/v24.13.0/bin"
declare -x NVM_CD_FLAGS=""
declare -x NVM_DIR="/home/asehgal/.nvm"
declare -x NVM_INC="/home/asehgal/.nvm/versions/node/v24.13.0/include/node"
declare -x OPENAI_API_BASE="http://localhost:30001/v1"
declare -x OPENAI_API_KEY="local"
declare -x PATH="/home/asehgal/.local/bin:/mnt/sdd1/atharvas/formulacode/datasmith_new/scratch/configs/codex_oss20b_config/tmp/arg0/codex-arg0Ziz4ZM:/home/asehgal/.nvm/versions/node/v24.13.0/lib/node_modules/@openai/codex/node_modules/@openai/codex-linux-x64/vendor/x86_64-unknown-linux-musl/path:/home/asehgal/.antigravity-server/bin/1.19.4-09171cd6214f9521f5202ffe72bda4443582da95/bin/remote-cli:/home/asehgal/.local/bin:/home/asehgal/.juliaup/bin:/home/asehgal/.cargo/bin:/home/asehgal/.nvm/versions/node/v24.13.0/bin:/mnt/sdd1/atharvas/env/miniconda3/bin:/mnt/sdd1/atharvas/env/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/asehgal/julia-1.6.7/bin:/home/asehgal/julia-1.6.7/bin/"
declare -x SHELL="/bin/bash"
declare -x SHLVL="2"
declare -x SSH_AUTH_SOCK="/home/asehgal/.antigravity-server/.09171cd6214f9521f5202ffe72bda4443582da95-ssh-auth.sock"
declare -x SSH_CLIENT="70.114.200.45 56887 22"
declare -x SSH_CONNECTION="70.114.200.45 56887 128.83.141.189 22"
declare -x TERM="xterm-256color"
declare -x TERM_PROGRAM="vscode"
declare -x TERM_PROGRAM_VERSION="1.107.0"
declare -x USER="asehgal"
declare -x UV_CACHE_DIR="/mnt/sdd3/atharvas/uv_cache"
declare -x VSCODE_GIT_ASKPASS_EXTRA_ARGS=""
declare -x VSCODE_GIT_ASKPASS_MAIN="/home/asehgal/.antigravity-server/bin/1.19.4-09171cd6214f9521f5202ffe72bda4443582da95/extensions/git/dist/askpass-main.js"
declare -x VSCODE_GIT_ASKPASS_NODE="/home/asehgal/.antigravity-server/bin/1.19.4-09171cd6214f9521f5202ffe72bda4443582da95/node"
declare -x VSCODE_GIT_IPC_HANDLE="/run/user/1007/vscode-git-0c4fca3c59.sock"
declare -x VSCODE_IPC_HOOK_CLI="/run/user/1007/vscode-ipc-20b3ac1c-1f54-4ba7-bc06-0300ca6553e8.sock"
declare -x VSCODE_PYTHON_AUTOACTIVATE_GUARD="1"
declare -x XDG_DATA_DIRS="/usr/local/share:/usr/share:/var/lib/snapd/desktop"
declare -x XDG_RUNTIME_DIR="/run/user/1007"
declare -x XDG_SESSION_CLASS="user"
declare -x XDG_SESSION_ID="7638"
declare -x XDG_SESSION_TYPE="tty"
declare -x _CE_CONDA=""
declare -x _CE_M=""
