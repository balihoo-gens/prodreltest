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

export AWS_REGION=${AWSREGION}

if [ "${DISTRO}" = "Ubuntu" ]; then
  INSTALLER="apt-get"
else
  INSTALLER="yum"
fi

log "installing dependencies"
if [ "${DISTRO}" = "Ubuntu" ]; then
  #remove existing lists, as update may fail with conflicting hash values
  # http://askubuntu.com/questions/553765/failed-to-fetch-update-on-ubuntu-14-04-lts-trusty-tahr
  logdo "rm -rf /var/lib/apt/lists/*"
  logdo "export DEBIAN_FRONTEND=noninteractive"
  #update the repo, but don't let it stop you; we're probably up to date enough anyway
  logdo "apt-get update -q || true"
  logdo "${INSTALLER} install -y unzip default-jre"
else
  log "nothing to install for amazon linux"
fi

log "downloading cli tools"
logdo "curl https://s3.amazonaws.com/aws-cli/awscli-bundle.zip -o awscli-bundle.zip"
logdo "unzip -o awscli-bundle.zip"
log "unzipped bundle"
logdo "./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws"
log "installed bundle"

S3APPURL="s3://${S3BUCKET}/${S3DIR}"
log "downloading fulfillment application"
logdo "mkdir -p ${FFDIR}"
logdo "/usr/local/bin/aws s3 sync ${S3APPURL} ${FFDIR}"

OSID=$(uname -srvm | sed "s/\W/_/g")
VEACTIVATE="source ${VEDIR}/activate"
VEARCHIVENAME="ve.tar.gz"
S3VEURL="s3://${S3BUCKET}/VirtualEnv/${OSID}/ve-${VEVERSION}/py-${PYVERSION}/${VEARCHIVENAME}"
#check for the presence of the VE check file on S3
log "checking ${S3VEURL}"
#force a true return value even if nothing is found, so that set -e doesn't bomb out
VEFILE="$(/usr/local/bin/aws s3 ls ${S3VEURL})" || true
if [ -z "${VEFILE}" ]; then
    log "no installation found: installing virtual env"
    if [ "${DISTRO}" = "Ubuntu" ]; then
        logdo "${INSTALLER} install -y gcc make autoconf libssl-dev libbz2-dev"
    else
        logdo "${INSTALLER} install -y gcc make autoconf openssl-devel zlib-devel"
    fi
    logdo "chmod +x ${FFDIR}/vesetup"
    logdo "${FFDIR}/vesetup -d ${VEDIR} -p ${PYVERSION} -v ${VEVERSION}"

    #install boto into this environment
    logdo "$VEACTIVATE"
    logdo "pip install boto"

    # recheck for the check file, in case someone was building simultaneously
    VEFILE="$(/usr/local/bin/aws s3 ls ${S3VEURL})" || true
    if [ -z "${VEFILE}" ]; then
        log "uploading virtual env install to S3"
        logdo "$(cd ${VEDIR} && tar -czf ${VEARCHIVENAME} *)"
        logdo "/usr/local/bin/aws s3 cp ${VEDIR}/${VEARCHIVENAME} ${S3VEURL}"
    else
        log "not uploading built version, another instance beat us to it"
    fi
else
    log "using virtual env version found on S3"
    logdo "/usr/local/bin/aws s3 cp ${S3VEURL} ${VEARCHIVENAME}"
    logdo "mkdir -p ${VEDIR}"
    logdo "tar -xzf ${VEARCHIVENAME} -C ${VEDIR}"
    logdo "$VEACTIVATE"
fi

if hostname | grep -q fulfillment; then echo 'hostname already set, skipping'; else hostname fulfillment-${ENV_NAME}-$(hostname); fi

FFINSTCMD="python ${FFDIR}/ffinstall.py ${EIPOPT} ${CLASSNAMES} --distro ${DISTRO}  --env ${ENV_NAME} ${NONEWRELIC_OPT} ${NOWORKER}"
echo "#!/bin/bash" > runffinstall
echo "${VEACTIVATE}" >> runffinstall
echo "${FFINSTCMD} >> ${LOGFILE} 2>&1" >> runffinstall
echo "deactivate" >> runffinstall
logdo "cat runffinstall"
logdo "chmod +x runffinstall"

log "installing fulfillment application"
nohup ./runffinstall > /tmp/ffinstall.log 2>&1&

log "done"


