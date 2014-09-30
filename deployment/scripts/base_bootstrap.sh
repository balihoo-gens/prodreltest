#set up vars and init logfiles
LOGFILE=/tmp/bootstrap.log
FFDIR=/opt/balihoo/fulfillment

log() {
    echo "$1" >> ${LOGFILE} 2>&1
}

logdo() {
    log "executing: $1 $2"
    $1 >> ${LOGFILE} 2>&1 $2
}
SRC="${BASH_SOURCE[0]}"
THIS="$(readlink -f $SRC)"
log "running bootstrap script: $THIS"
log "PATH: ${PATH}"

log "export aws keys"
export AWS_ACCESS_KEY_ID=${AWSACCESSKEY}
export AWS_SECRET_ACCESS_KEY=${AWSSECRETKEY}
export AWS_REGION=${AWSREGION}

log "installing dependencies"
logdo "export DEBIAN_FRONTEND=noninteractive"
logdo "apt-get update -q"
logdo "apt-get install -y unzip gcc make autoconf libssl-dev default-jre"

log "downloading cli tools"
logdo "curl https://s3.amazonaws.com/aws-cli/awscli-bundle.zip -o awscli-bundle.zip"
logdo "unzip -o awscli-bundle.zip"
log "unzipped bundle"
logdo "./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws"
log "installed bundle"

log "downloading fulfillment application"
logdo "mkdir -p ${FFDIR}"
logdo "/usr/local/bin/aws s3 sync ${S3BUCKETURL} ${FFDIR}"

log "installing virtual env"
logdo "chmod +x ${FFDIR}/vesetup"
logdo "${FFDIR}/vesetup -d ${VEDIR} -p ${PYVERSION} -v ${VEVERSION}"

log "activating virtual env"
ACTIVATE="source ${VEDIR}/activate"
logdo "$ACTIVATE"

log "installing boto"
logdo "pip install boto"

FFINSTCMD="python ${FFDIR}/ffinstall ${EIPOPT} ${CLASSNAMES}"
echo "#!/bin/bash" > runffinstall
echo "$ACTIVATE" >> runffinstall
echo "$FFINSTCMD" >> runffinstall
echo "deactivate" >> runffinstall
logdo "cat runffinstall"
logdo "chmod +x runffinstall"

log "installing fulfillment application"
nohup ./runffinstall > /tmp/ffinstall.log 2>&1&

log "done"


