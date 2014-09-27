#set up vars and init logfiles
LOGFILE=/tmp/bootstrap.log
FFDIR=/opt/balihoo/fulfillment
echo "running bootstrap script" > ${LOGFILE} 2>&1
echo "PATH: ${PATH}" >> ${LOGFILE} 2>&1

log() {
    echo "$1" >> ${LOGFILE} 2>&1
}

logdo() {
    log "executing: $1 $2"
    $1 >> ${LOGFILE} 2>&1 $2
}

log "export aws keys"
export AWS_ACCESS_KEY_ID=${AWSACCESSKEY}
export AWS_SECRET_ACCESS_KEY=${AWSSECRETKEY}
export AWS_REGION=${AWSREGION}

log "downloading cli tools"
logdo "curl https://s3.amazonaws.com/aws-cli/awscli-bundle.zip -o awscli-bundle.zip"
logdo "unzip awscli-bundle.zip"
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
logdo "source ${VEDIR}/activate"

log "installing boto"
logdo "pip install boto"

log "installing fulfillment application"
logdo "nohup python ${FFDIR}/ffinstall ${EIPOPT} ${CLASSNAMES}" "&"

log "done"


